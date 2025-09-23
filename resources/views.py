# resources/views.py
import json
import threading
import traceback
from datetime import datetime
from django.shortcuts import render, redirect
from django.http import JsonResponse, HttpResponseForbidden, Http404
from django.contrib.auth.decorators import login_required
from django.db import connection, transaction
from django.core.paginator import Paginator
from django.views.decorators.http import require_http_methods
from django.conf import settings
import logging
import traceback
import time
import hashlib

logger = logging.getLogger(__name__)

# configuration: tune as needed
_LDAP_PAGE_SIZE = getattr(settings, "LDAP_SYNC_PAGE_SIZE", 500)   # page size for paged search
_PROGRESS_UPDATE_EVERY = getattr(settings, "LDAP_SYNC_PROGRESS_BATCH", 25)  # update job every N rows

from accounts.ldap_utils import _get_ldap_connection  # binds with credentials if provided
from accounts.ldap_utils import get_reportees_for_user_dn, get_user_entry_by_username

# ---------------------------
# Helpers
# ---------------------------
def _create_sync_job(started_by):
    with connection.cursor() as cur:
        cur.execute(
            "INSERT INTO ldap_sync_jobs (started_by, status, total_count, processed_count, errors_count) VALUES (%s,%s,0,0,0)",
            (started_by, "PENDING"),
        )
        cur.execute("SELECT LAST_INSERT_ID()")
        row = cur.fetchone()
        return row[0] if row else None

def _update_sync_job(job_id, **kwargs):
    # allowed: status, total_count, processed_count, errors_count, details, finished_at
    set_parts = []
    params = []
    for k, v in kwargs.items():
        if k == "finished_at":
            set_parts.append("finished_at = %s")
            params.append(v)
        elif k in ("status", "total_count", "processed_count", "errors_count", "details"):
            set_parts.append(f"`{k}` = %s")
            params.append(v)
    if not set_parts:
        return
    sql = f"UPDATE ldap_sync_jobs SET {', '.join(set_parts)}, updated_at = CURRENT_TIMESTAMP WHERE id = %s"
    params.append(job_id)
    with connection.cursor() as cur:
        cur.execute(sql, params)

def _upsert_ldap_user_row(attrs):
    """
    Upsert a single LDAP entry into ldap_directory.
    attrs: dict with keys from LDAP. Must compute ldap_dn_hash and include it.
    """
    try:
        username = attrs.get("sAMAccountName") or attrs.get("userPrincipalName") or attrs.get("username") or None
        ldap_dn = attrs.get("dn") or attrs.get("ldap_dn") or ""
        ldap_dn_hash = _sha256_hex(ldap_dn)

        email = attrs.get("mail") or attrs.get("email") or None
        cn = attrs.get("cn") or None
        givenName = attrs.get("givenName") or attrs.get("given_name") or None
        sn = attrs.get("sn") or None
        title = attrs.get("title") or None
        department = attrs.get("department") or None
        tel = attrs.get("telephoneNumber") or None
        mobile = attrs.get("mobile") or None
        manager_dn = attrs.get("manager") or attrs.get("manager_dn") or None

        # Ensure attributes_json is a JSON string
        try:
            attributes_json = json.dumps(attrs, default=str)
        except Exception:
            attributes_json = "{}"

        sql = """
        INSERT INTO ldap_directory
          (username, email, cn, givenName, sn, title, department, telephoneNumber, mobile, manager_dn, ldap_dn, ldap_dn_hash, attributes_json)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          username = VALUES(username),
          email = VALUES(email),
          cn = VALUES(cn),
          givenName = VALUES(givenName),
          sn = VALUES(sn),
          title = VALUES(title),
          department = VALUES(department),
          telephoneNumber = VALUES(telephoneNumber),
          mobile = VALUES(mobile),
          manager_dn = VALUES(manager_dn),
          attributes_json = VALUES(attributes_json),
          ldap_dn_hash = VALUES(ldap_dn_hash),
          updated_at = CURRENT_TIMESTAMP
        """
        params = (
            username, email, cn, givenName, sn, title, department, tel, mobile, manager_dn,
            ldap_dn, ldap_dn_hash, attributes_json
        )

        with connection.cursor() as cur:
            cur.execute(sql, params)

    except Exception as ex:
        # log and re-raise so caller can increment errors_count
        logger.exception("Failed to upsert ldap_directory row for dn=%s: %s", ldap_dn, ex)
        raise


# ---------------------------
# LDAP sync worker (runs in a background thread)
# ---------------------------


def _sha256_hex(s: str) -> str:
    if s is None:
        s = ""
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

# Python
def _full_ldap_sync_worker(job_id, ldap_username, ldap_password):
    """
    Improved full-sync worker:
    - uses paged_search if available (ldap3)
    - upserts entries into ldap_directory
    - writes regular progress updates
    - on exception marks job FAILED and stores traceback into details
    """
    try:
        print(f"LDAP sync worker starting job_id={job_id} by session_user={ldap_username}")
        _update_sync_job(job_id, status="RUNNING", processed_count=0, errors_count=0, details=None)

        # bind (use provided creds if given, else fallback in ldap_utils)
        try:
            print("Attempting LDAP bind...")
            conn = _get_ldap_connection(username=ldap_username, password=ldap_password) if ldap_username else _get_ldap_connection()
            print("LDAP bind successful.")
        except Exception as e:
            print(f"LDAP bind failed for job {job_id}: {e}")
            _update_sync_job(job_id, status="FAILED", details=f"LDAP bind failed: {str(e)}")
            return

        # determine search base and attributes
        user_search_base = getattr(settings, "LDAP_USER_SEARCH_BASE", "")
        base_dn = getattr(settings, "LDAP_BASE_DN", "")
        search_base = f"{user_search_base},{base_dn}" if user_search_base else base_dn
        print(f"LDAP search base: {search_base}")

        attributes = getattr(settings, "LDAP_ATTRIBUTES", [
            'cn', 'sAMAccountName', 'userPrincipalName', 'mail', 'department',
            'title', 'telephoneNumber', 'givenName', 'sn', 'memberOf', 'manager'
        ])
        print(f"LDAP attributes: {attributes}")

        filter_str = getattr(settings, "LDAP_FULL_SYNC_FILTER", "(|(objectClass=person)(objectClass=user))")
        print(f"LDAP filter string: {filter_str}")

        processed = 0
        errors = 0
        total_estimate = 0

        # Try paged_search (generator) — best for large directories
        used_generator = False
        try:
            print("Checking for paged_search support...")
            if hasattr(conn, "extend") and hasattr(conn.extend, "standard") and hasattr(conn.extend.standard, "paged_search"):
                used_generator = True
                print("Using paged_search generator.")
                entries_gen = conn.extend.standard.paged_search(
                    search_base=search_base,
                    search_filter=filter_str,
                    search_scope='SUBTREE',
                    attributes=attributes,
                    paged_size=_LDAP_PAGE_SIZE,
                    generator=True
                )
                _update_sync_job(job_id, total_count=0)
                batch_since_update = 0
                for entry in entries_gen:
                    try:
                        ent = {}
                        if isinstance(entry, dict):
                            ent["dn"] = entry.get("dn") or entry.get("entry_dn") or ""
                            attrs = entry.get("attributes") or {}
                            for k, v in attrs.items():
                                ent[k] = v
                        else:
                            ent["dn"] = str(getattr(entry, "entry_dn", "")) if hasattr(entry, "entry_dn") else ""
                            for a in attributes:
                                try:
                                    val = getattr(entry, a, None)
                                    if val is None:
                                        ent[a] = None
                                    else:
                                        if hasattr(val, "value"):
                                            ent[a] = str(val.value)
                                        else:
                                            try:
                                                ent[a] = list(val.values) if hasattr(val, "values") else str(val)
                                            except Exception:
                                                ent[a] = str(val)
                                except Exception as attr_ex:
                                    print(f"Error extracting attribute '{a}' from entry: {attr_ex}")
                                    ent[a] = None

                        ldap_dn = ent.get("dn") or ent.get("ldap_dn") or ""
                        ent["ldap_dn"] = ldap_dn
                        ent["ldap_dn_hash"] = _sha256_hex(ldap_dn)
                        _upsert_ldap_user_row(ent)

                        processed += 1
                        batch_since_update += 1
                        if batch_since_update >= _PROGRESS_UPDATE_EVERY:
                            print(f"Processed {processed} entries so far (paged_search)")
                            _update_sync_job(job_id, processed_count=processed)
                            batch_since_update = 0
                    except Exception as entry_ex:
                        errors += 1
                        if errors % 10 == 0:
                            print(f"Encountered {errors} errors so far (paged_search)")
                            _update_sync_job(job_id, errors_count=errors)
                        print(f"Error processing LDAP entry during job {job_id}: {entry_ex}")
                print(f"Paged_search complete. Processed={processed}, Errors={errors}")
                _update_sync_job(job_id, processed_count=processed, errors_count=errors, status="COMPLETED", finished_at=datetime.utcnow())
                try:
                    conn.unbind()
                    print("LDAP connection unbound successfully (paged_search).")
                except Exception as unbind_ex:
                    print(f"Error unbinding LDAP connection: {unbind_ex}")
                print(f"LDAP sync job {job_id} completed: processed={processed} errors={errors}")
                return

        except Exception as pg_ex:
            print(f"Paged search failed/unsupported for job {job_id}: {pg_ex}. Falling back to non-paged search.")

        # Fallback: non-paged search (be careful with large result sets)
        try:
            print("Starting fallback LDAP search...")
            ok = conn.search(search_base=search_base, search_filter=filter_str, search_scope='SUBTREE', attributes=attributes)
            if not ok:
                print(f"LDAP search returned no results or false for job {job_id}")
            entries = list(conn.entries)
            total_estimate = len(entries)
            print(f"LDAP search complete. Total entries found: {total_estimate}")
            _update_sync_job(job_id, total_count=total_estimate)
            batch_since_update = 0

            for e in entries:
                try:
                    ent = {}
                    ent["dn"] = str(getattr(e, "entry_dn", "")) if hasattr(e, "entry_dn") else ""
                    for a in attributes:
                        try:
                            val = getattr(e, a, None)
                            if val is None:
                                ent[a] = None
                            else:
                                if hasattr(val, "value"):
                                    ent[a] = str(val.value)
                                else:
                                    try:
                                        ent[a] = list(val.values) if hasattr(val, "values") else str(val)
                                    except Exception:
                                        ent[a] = str(val)
                        except Exception as attr_ex:
                            print(f"Error extracting attribute '{a}' from entry: {attr_ex}")
                            ent[a] = None

                    ent["ldap_dn"] = ent.get("dn", "")
                    ent["ldap_dn_hash"] = _sha256_hex(ent["ldap_dn"])
                    _upsert_ldap_user_row(ent)
                    processed += 1
                    batch_since_update += 1
                    if batch_since_update >= _PROGRESS_UPDATE_EVERY:
                        print(f"Processed {processed} entries so far (fallback search)")
                        _update_sync_job(job_id, processed_count=processed)
                        batch_since_update = 0
                except Exception as entry_ex:
                    errors += 1
                    if errors % 10 == 0:
                        print(f"Encountered {errors} errors so far (fallback search)")
                        _update_sync_job(job_id, errors_count=errors)
                    print(f"Error processing LDAP entry in fallback loop for job {job_id}: {entry_ex}")

            print(f"Fallback search complete. Processed={processed}, Errors={errors}")
            _update_sync_job(job_id, processed_count=processed, errors_count=errors, status="COMPLETED", finished_at=datetime.utcnow())
            try:
                conn.unbind()
                print("LDAP connection unbound successfully (fallback search).")
            except Exception as unbind_ex:
                print(f"Error unbinding LDAP connection: {unbind_ex}")
            print(f"LDAP sync job {job_id} completed (fallback): processed={processed} errors={errors}")
            return

        except Exception as fallback_ex:
            print(f"Exception during fallback LDAP search for job {job_id}: {fallback_ex}")

    except Exception as top_ex:
        tb = traceback.format_exc()
        print(f"Unhandled exception in ldap sync worker for job {job_id}: {top_ex}")
        print(tb)
        try:
            _update_sync_job(job_id, status="FAILED", details=tb, finished_at=datetime.utcnow())
        except Exception as update_ex:
            print(f"Failed to update sync job {job_id} to FAILED after exception: {update_ex}")
        return



# ---------------------------
# Views
# ---------------------------


def redirect_to_directory(request):
    return redirect("resources:directory")



def ldap_sync_page(request):
    """Render the sync page with a single button and an empty progress area."""
    # show last 5 jobs for history
    with connection.cursor() as cur:
        cur.execute("SELECT id, started_at, finished_at, started_by, status, total_count, processed_count, errors_count FROM ldap_sync_jobs ORDER BY id DESC LIMIT 5")
        cols = [c[0] for c in cur.description] if cur.description else []
        jobs = [dict(zip(cols, r)) for r in cur.fetchall()] if cols else []
    return render(request, "resources/ldap_sync.html", {"jobs": jobs})

# Add helper to read allowed roles from settings with sensible default
def _allowed_ldap_sync_roles():
    # e.g. set in settings.py: LDAP_SYNC_ALLOWED_ROLES = ["ADMIN", "PDL"]
    roles = getattr(settings, "LDAP_SYNC_ALLOWED_ROLES", ["ADMIN","PDL"])
    # normalize to upper-case strings
    return {str(r).upper() for r in roles}

# replacement ldap_sync_start view
from django.views.decorators.http import require_POST
from django.contrib.auth.decorators import login_required

@require_POST
def ldap_sync_start(request):
    """
    Create a job row and start a background thread to sync the entire LDAP directory.
    Permission to start the sync is controlled by settings.LDAP_SYNC_ALLOWED_ROLES (defaults to ["ADMIN"]).
    """
    role = str(request.session.get("role", "EMPLOYEE") or "EMPLOYEE").upper()
    allowed = _allowed_ldap_sync_roles()
    print("LDAP sync start: user=%s session_role=%s allowed_roles=%s", request.session.get("username"), role, allowed)

    if role not in allowed:
        # return JSON for AJAX caller, and 403 status for clarity
        msg = f"Permission denied: only roles {sorted(list(allowed))} can start full sync"
        print("LDAP sync start forbidden for user=%s (role=%s)", request.session.get("ldap_username"), role)
        return JsonResponse({"ok": False, "error": msg}, status=403)

    # proceed to create job and start background thread
    ldap_user = request.session.get("ldap_username")
    ldap_pw = request.session.get("ldap_password")  # as you store in session
    started_by = request.session.get("username") or ldap_user or request.user.username

    job_id = _create_sync_job(started_by)
    if not job_id:
        print("Could not create ldap_sync job for user=%s", started_by)
        return JsonResponse({"ok": False, "error": "Could not create job"}, status=500)

    # start background thread
    try:
        t = threading.Thread(target=_full_ldap_sync_worker, args=(job_id, ldap_user, ldap_pw), daemon=True)
        t.start()
    except Exception as ex:
        print("Failed to start background thread for ldap sync job %s", job_id)
        return JsonResponse({"ok": False, "error": "Failed to start sync worker"}, status=500)

    print("LDAP sync job %s started by %s (role=%s)", job_id, started_by, role)
    return JsonResponse({"ok": True, "job_id": job_id})

def ldap_sync_progress(request):
    """Return JSON progress for given job_id."""
    job_id = request.GET.get("job_id")
    if not job_id:
        return JsonResponse({"ok": False, "error": "job_id required"}, status=400)
    with connection.cursor() as cur:
        cur.execute("SELECT id, started_at, finished_at, started_by, status, total_count, processed_count, errors_count, details FROM ldap_sync_jobs WHERE id = %s", (job_id,))
        row = cur.fetchone()
        if not row:
            return JsonResponse({"ok": False, "error": "job not found"}, status=404)
        cols = [c[0] for c in cur.description]
        job = dict(zip(cols, row))
    return JsonResponse({"ok": True, "job": job})



def employee_directory(request):
    """
    Local employee directory page:
    - shows total synced rows
    - shows employees that are referenced in `users` table (i.e. used in projects)
    - paginated list (15 per page), up to 200 entries (cap)
    - server-side search for q param (if provided)
    """
    q = (request.GET.get("q") or "").strip()
    page_no = int(request.GET.get("page") or 1)
    per_page = 15
    cap = 200

    # total synced count
    with connection.cursor() as cur:
        cur.execute("SELECT COUNT(1) FROM ldap_directory")
        total_synced = cur.fetchone()[0]

    # employees referenced in users table (join ldap_directory.username = users.username)
    # limited to cap, optionally filtered by q
    params = []
    where = ""
    if q and len(q) >= 3:
        where = "AND (ld.username LIKE %s OR ld.cn LIKE %s OR ld.email LIKE %s)"
        like = f"%{q}%"
        params.extend([like, like, like])

    # join to users to show used employees - the requirement mentions "LDAP details of used employees in project until now (reference table is users)"
    sql = f"""
    SELECT ld.id, ld.username, ld.email, ld.cn, ld.title, ld.department
    FROM ldap_directory ld
    JOIN users u ON (u.username = ld.username)
    WHERE 1=1 {where}
    LIMIT %s
    """
    params.append(cap)
    with connection.cursor() as cur:
        cur.execute(sql, params)
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    # paginate the rows server-side
    paginator = Paginator(rows, per_page)
    try:
        page = paginator.page(page_no)
    except:
        page = paginator.page(1)

    return render(request, "resources/directory.html", {
        "total_synced": total_synced,
        "employees_page": page,
        "q": q,
        "paginator": paginator,
    })



def ldap_local_search_api(request):
    """AJAX: search the local ldap_directory for q (min 3 chars) and return up to 20 matches."""
    q = (request.GET.get("q") or "").strip()
    if len(q) < 3:
        return JsonResponse({"results": []})
    like = f"{q}%"
    with connection.cursor() as cur:
        cur.execute("""
            SELECT id, username, cn, email, title, department
            FROM ldap_directory
            WHERE username LIKE %s OR cn LIKE %s OR email LIKE %s
            ORDER BY username
            LIMIT 20
        """, (like, like, like))
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return JsonResponse({"results": rows})



def ldap_local_profile_api(request, ld_id):
    """AJAX: return full attributes_json for a local ldap entry id."""
    with connection.cursor() as cur:
        cur.execute("SELECT id, username, email, cn, attributes_json FROM ldap_directory WHERE id = %s LIMIT 1", (ld_id,))
        row = cur.fetchone()
        if not row:
            raise Http404("not found")
        cols = [c[0] for c in cur.description]
        rec = dict(zip(cols, row))
        # attributes_json may be TEXT/JSON - ensure it's parsed
        try:
            rec["attributes"] = json.loads(rec.get("attributes_json") or "{}")
        except Exception:
            rec["attributes"] = {}
    return JsonResponse({"ok": True, "record": rec})
