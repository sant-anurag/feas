# projects/views.py
import logging
from django.shortcuts import render, redirect
from django.conf import settings
from django.urls import reverse
from django.http import JsonResponse, HttpResponseBadRequest, HttpResponseNotAllowed
from django.views.decorators.http import require_GET, require_POST
import mysql.connector
from mysql.connector import Error, IntegrityError
import json
from datetime import datetime
from django.shortcuts import render
from django.http import JsonResponse, HttpResponseBadRequest, HttpResponseForbidden
from django.db import connection, transaction
from django.views.decorators.http import require_http_methods
from django.contrib.auth.decorators import login_required

# -------------------------
# LDAP helpers (use your ldap_utils)
# -------------------------
# We expect these functions to be provided in accounts.ldap_utils and accept optional
# username_password_for_conn param so they can use session credentials.
try:
    from accounts.ldap_utils import get_user_entry_by_username, get_reportees_for_user_dn
except Exception:
    # Provide simple fallbacks that return None/empty to avoid crashes if ldap_utils missing.
    def get_user_entry_by_username(username, username_password_for_conn=None):
        logger.warning("ldap_utils.get_user_entry_by_username not available")
        return None

    def get_reportees_for_user_dn(user_dn, username_password_for_conn=None):
        logger.warning("ldap_utils.get_reportees_for_user_dn not available")
        return []

logger = logging.getLogger(__name__)

# Default hours available per employee per month (can be overridden in settings)
HOURS_AVAILABLE_PER_MONTH = float(getattr(settings, "HOURS_AVAILABLE_PER_MONTH", 183.75))

# -------------------------
# DB helpers
# -------------------------
def dictfetchall(cursor):
    """Return all rows from a cursor as a list of dicts."""
    cols = [c[0] for c in cursor.description] if cursor.description else []
    return [dict(zip(cols, row)) for row in cursor.fetchall()]

def get_connection():
    dbs = settings.DATABASES.get("default", {})
    return mysql.connector.connect(
        host=dbs.get("HOST", "127.0.0.1") or "127.0.0.1",
        port=int(dbs.get("PORT", 3306) or 3306),
        user=dbs.get("USER", "root") or "",
        password=dbs.get("PASSWORD", "root") or "",
        database=dbs.get("NAME", "feasdb") or "",
        charset="utf8mb4",
        use_unicode=True,
    )


def _ensure_user_from_ldap(request,samaccountname):
    if not samaccountname:
        return None
    sam = request.session.get("ldap_username")
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT id FROM users WHERE ldap_id = %s OR username = %s LIMIT 1", (sam, sam))
        row = cur.fetchone()
        if row:
            return row["id"]
        ins = conn.cursor()
        ins.execute("INSERT INTO users (username, ldap_id) VALUES (%s, %s)", (sam, sam))
        conn.commit()
        nid = ins.lastrowid
        ins.close()
        return nid
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def _fetch_users():
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT id, username, email FROM users ORDER BY username LIMIT 500")
        return cur.fetchall()
    finally:
        cur.close(); conn.close()


def _fetch_project(project_id):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT * FROM projects WHERE id=%s LIMIT 1", (project_id,))
        return cur.fetchone()
    finally:
        cur.close(); conn.close()


@require_GET
def project_list(request):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT p.id, p.name, p.description, p.start_date, p.end_date, u.username as pdl_username
            FROM projects p
            LEFT JOIN users u ON p.pdl_user_id = u.id
            ORDER BY p.created_at DESC
        """)
        projects = cur.fetchall()
    finally:
        cur.close(); conn.close()
    return render(request, "projects/project_list.html", {"projects": projects})


def _get_all_coes():
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT id, name FROM coes ORDER BY name")
        return cur.fetchall()
    finally:
        cur.close(); conn.close()


def _assign_coes_to_project(project_id, coe_ids):
    """
    Given project_id and iterable of coe_ids, insert into project_coes table.
    This function is idempotent: it skips existing mappings and inserts new ones.
    """
    if not coe_ids:
        return
    conn = get_connection()
    cur = conn.cursor()
    try:
        for cid in coe_ids:
            try:
                cur.execute("INSERT INTO project_coes (project_id, coe_id) VALUES (%s, %s)", (project_id, cid))
                # commit per batch later
            except IntegrityError:
                # mapping exists — ignore
                continue
        conn.commit()
    finally:
        cur.close(); conn.close()


def _replace_project_coes(project_id, coe_ids):
    """
    Replace mappings for project: delete all existing and insert provided list (idempotent).
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM project_coes WHERE project_id=%s", (project_id,))
        if coe_ids:
            for cid in coe_ids:
                try:
                    cur.execute("INSERT INTO project_coes (project_id, coe_id) VALUES (%s, %s)", (project_id, cid))
                except IntegrityError:
                    continue
        conn.commit()
    finally:
        cur.close(); conn.close()


@require_POST
def delete_project(request, project_id):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM projects WHERE id=%s", (project_id,))
        conn.commit()
    finally:
        cur.close(); conn.close()
    return redirect(reverse("projects:list"))

@require_POST
def create_coe(request):
    name = (request.POST.get("name") or "").strip()
    leader_username = request.POST.get("leader_username") or None
    description = request.POST.get("description") or None

    if not name:
        return HttpResponseBadRequest("COE name required")

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT id FROM coes WHERE name = %s LIMIT 1", (name,))
        if cur.fetchone():
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse({"success": False, "error": "COE with this name already exists."}, status=400)
            return redirect(request.META.get("HTTP_REFERER", reverse("projects:create")))
    finally:
        cur.close(); conn.close()

    leader_user_id = None
    if leader_username:
        leader_user_id = _ensure_user_from_ldap(request,leader_username)

    conn2 = get_connection()
    cur2 = conn2.cursor()
    try:
        try:
            cur2.execute("INSERT INTO coes (name, leader_user_id, description) VALUES (%s, %s, %s)",
                         (name, leader_user_id, description))
            conn2.commit()
        except IntegrityError as e:
            logger.warning("create_coe IntegrityError: %s", e)
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse({"success": False, "error": "COE insert failed (duplicate)."}, status=400)
            return redirect(request.META.get("HTTP_REFERER", reverse("projects:create")))
    finally:
        cur2.close(); conn2.close()

    if request.headers.get("x-requested-with") == "XMLHttpRequest":
        return JsonResponse({"success": True})
    return redirect(request.META.get("HTTP_REFERER", reverse("projects:create")))

@require_POST
def edit_coe(request, coe_id):
    name = (request.POST.get("name") or "").strip()
    leader_username = request.POST.get("leader_username") or None
    description = request.POST.get("description") or None

    if not name:
        return HttpResponseBadRequest("COE name required")

    leader_user_id = None
    if leader_username:
        leader_user_id = _ensure_user_from_ldap(request,leader_username)

    conn = get_connection()
    cur = conn.cursor()
    try:
        try:
            cur.execute("UPDATE coes SET name=%s, leader_user_id=%s, description=%s WHERE id=%s",
                        (name, leader_user_id, description, coe_id))
            conn.commit()
        except IntegrityError as e:
            logger.warning("edit_coe IntegrityError: %s", e)
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse({"success": False, "error": "COE update failed (duplicate or constraint)."}, status=400)
            return redirect(request.META.get("HTTP_REFERER", reverse("projects:create")))
    finally:
        cur.close(); conn.close()

    if request.headers.get("x-requested-with") == "XMLHttpRequest":
        return JsonResponse({"success": True})
    return redirect(request.META.get("HTTP_REFERER", reverse("projects:create")))


@require_POST
def create_domain(request):
    name = (request.POST.get("name") or "").strip()
    coe_id = request.POST.get("coe_id") or None
    lead_username = request.POST.get("lead_username") or None

    if not name:
        return HttpResponseBadRequest("Domain name required")

    try:
        coe_id_int = int(coe_id) if coe_id else None
    except Exception:
        coe_id_int = None

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT id FROM domains WHERE coe_id = %s AND name = %s LIMIT 1", (coe_id_int, name))
        if cur.fetchone():
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse({"success": False, "error": "Domain with this name already exists for the selected COE."}, status=400)
            return redirect(request.META.get("HTTP_REFERER", reverse("projects:create")))
    finally:
        cur.close(); conn.close()

    lead_user_id = None
    if lead_username:
        lead_user_id = _ensure_user_from_ldap(request,lead_username)

    conn2 = get_connection()
    cur2 = conn2.cursor()
    try:
        try:
            cur2.execute("INSERT INTO domains (coe_id, name, lead_user_id) VALUES (%s, %s, %s)",
                         (coe_id_int if coe_id_int else None, name, lead_user_id))
            conn2.commit()
        except IntegrityError as e:
            logger.warning("create_domain IntegrityError: %s", e)
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse({"success": False, "error": "Domain insert failed (duplicate)."}, status=400)
            return redirect(request.META.get("HTTP_REFERER", reverse("projects:create")))
    finally:
        cur2.close(); conn2.close()

    if request.headers.get("x-requested-with") == "XMLHttpRequest":
        return JsonResponse({"success": True})
    return redirect(request.META.get("HTTP_REFERER", reverse("projects:create")))


@require_POST
def edit_domain(request, domain_id):
    name = (request.POST.get("name") or "").strip()
    coe_id = request.POST.get("coe_id") or None
    lead_username = request.POST.get("lead_username") or None

    if not name:
        return HttpResponseBadRequest("Domain name required")

    try:
        coe_id_int = int(coe_id) if coe_id else None
    except Exception:
        coe_id_int = None

    lead_user_id = None
    if lead_username:
        lead_user_id = _ensure_user_from_ldap(request,lead_username)

    conn = get_connection()
    cur = conn.cursor()
    try:
        try:
            cur.execute("UPDATE domains SET coe_id=%s, name=%s, lead_user_id=%s WHERE id=%s",
                        (coe_id_int if coe_id_int else None, name, lead_user_id, domain_id))
            conn.commit()
        except IntegrityError as e:
            logger.warning("edit_domain IntegrityError: %s", e)
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse({"success": False, "error": "Domain update failed (duplicate or constraint)."}, status=400)
            return redirect(request.META.get("HTTP_REFERER", reverse("projects:create")))
    finally:
        cur.close(); conn.close()

    if request.headers.get("x-requested-with") == "XMLHttpRequest":
        return JsonResponse({"success": True})
    return redirect(request.META.get("HTTP_REFERER", reverse("projects:create")))


@require_GET
def ldap_search(request):
    q = (request.GET.get("q") or "").strip()
    if len(q) < 1:
        return JsonResponse({"results": []})

    results = []
    try:
        from accounts import ldap_utils
        username = request.session.get("ldap_username")
        password = request.session.get("ldap_password")
        conn = ldap_utils._get_ldap_connection(username, password)
        base_dn = getattr(settings, "LDAP_BASE_DN", "")
        conn.search(
            search_base=base_dn,
            search_filter=f"(|(sAMAccountName=*{q}*)(cn=*{q}*)(mail=*{q}*))",
            search_scope='SUBTREE',
            attributes=['sAMAccountName', 'mail', 'cn', 'title']
        )
        for e in conn.entries:
            results.append({
                "sAMAccountName": str(getattr(e, 'sAMAccountName', '')),
                "mail": str(getattr(e, 'mail', '')),
                "cn": str(getattr(e, 'cn', '')),
                "title": str(getattr(e, 'title', '')),
            })
        try:
            conn.unbind()
        except Exception:
            pass
    except Exception as ex:
        logger.warning("LDAP search failed, falling back to users table: %s", ex)
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        try:
            like = f"%{q}%"
            cur.execute(
                "SELECT username as sAMAccountName, email as mail, username as cn "
                "FROM users WHERE username LIKE %s OR email LIKE %s LIMIT 40",
                (like, like)
            )
            rows = cur.fetchall()
            for r in rows:
                results.append({
                    "sAMAccountName": r.get("sAMAccountName"),
                    "mail": r.get("mail"),
                    "cn": r.get("cn"),
                    "title": ""
                })
        finally:
            cur.close(); conn.close()

    return JsonResponse({"results": results})


def _get_all_projects(limit=200):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT id, name FROM projects ORDER BY created_at DESC LIMIT %s", (limit,))
        return cur.fetchall()
    finally:
        cur.close(); conn.close()

def _get_project_coe_ids(project_id):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT coe_id FROM project_coes WHERE project_id=%s", (project_id,))
        rows = cur.fetchall()
        return [r['coe_id'] for r in rows] if rows else []
    finally:
        cur.close(); conn.close()


@require_GET
def project_list(request):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT p.id, p.name, p.description, p.start_date, p.end_date, u.username as pdl_username
            FROM projects p
            LEFT JOIN users u ON p.pdl_user_id = u.id
            ORDER BY p.created_at DESC
        """)
        projects = cur.fetchall()
    finally:
        cur.close(); conn.close()
    # compute mapped counts
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT project_id, COUNT(*) as cnt FROM project_coes GROUP BY project_id")
        rows = cur.fetchall()
        counts = {r['project_id']: r['cnt'] for r in rows} if rows else {}
    finally:
        cur.close(); conn.close()
    for p in projects:
        p['mapped_coe_count'] = counts.get(p['id'], 0)
    return render(request, "projects/project_list.html", {"projects": projects})

def create_project(request):
    if request.method == "POST":
        name = (request.POST.get("name") or "").strip()
        desc = (request.POST.get("description") or "").strip()
        start_date = request.POST.get("start_date") or None
        end_date = request.POST.get("end_date") or None
        pdl_username = request.POST.get("pdl_username") or None
        mapped_coe_ids = request.POST.getlist("mapped_coe_ids")

        if not name:
            users = _fetch_users()
            coes = _get_all_coes()
            projects = _get_all_projects()
            conn = get_connection()
            cur = conn.cursor(dictionary=True)
            try:
                cur.execute("SELECT id, name, coe_id FROM domains ORDER BY name")
                domains = cur.fetchall()
            finally:
                cur.close(); conn.close()
            return render(request, "projects/create_project.html", {
                "users": users, "coes": coes, "projects": projects, "domains": domains, "error": "Project name is required."
            })

        pdl_user_id = None
        if pdl_username:
            pdl_user_id = _ensure_user_from_ldap(request,pdl_username)

        conn = get_connection()
        cur = conn.cursor()
        project_id = None
        try:
            cur.execute(
                "INSERT INTO projects (name, description, start_date, end_date, pdl_user_id) VALUES (%s, %s, %s, %s, %s)",
                (name, desc or None, start_date, end_date, pdl_user_id)
            )
            conn.commit()
            project_id = cur.lastrowid
        finally:
            cur.close(); conn.close()

        try:
            int_coe_ids = [int(x) for x in mapped_coe_ids if x]
        except Exception:
            int_coe_ids = []
        if project_id and int_coe_ids:
            _replace_project_coes(project_id, int_coe_ids)

        if request.headers.get("x-requested-with") == "XMLHttpRequest":
            return JsonResponse({"success": True, "project_id": project_id})
        return redirect(reverse("projects:list"))

    users = _fetch_users()
    coes = _get_all_coes()
    projects = _get_all_projects()
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT id, name, coe_id FROM domains ORDER BY name")
        domains = cur.fetchall()
    finally:
        cur.close(); conn.close()

    return render(request, "projects/create_project.html", {
        "users": users, "coes": coes, "projects": projects, "domains": domains
    })

def edit_project(request, project_id):
    if request.method == "POST":
        name = (request.POST.get("name") or "").strip()
        desc = (request.POST.get("description") or "").strip()
        start_date = request.POST.get("start_date") or None
        end_date = request.POST.get("end_date") or None
        pdl_username = request.POST.get("pdl_username") or None
        mapped_coe_ids = request.POST.getlist("mapped_coe_ids")

        if not name:
            return HttpResponseBadRequest("Project name required")

        pdl_user_id = None
        if pdl_username:
            pdl_user_id = _ensure_user_from_ldap(request,pdl_username)

        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                UPDATE projects SET name=%s, description=%s, start_date=%s, end_date=%s, pdl_user_id=%s WHERE id=%s
            """, (name, desc or None, start_date, end_date, pdl_user_id, project_id))
            conn.commit()
        finally:
            cur.close(); conn.close()

        try:
            int_coe_ids = [int(x) for x in mapped_coe_ids if x]
        except Exception:
            int_coe_ids = []
        _replace_project_coes(project_id, int_coe_ids)

        return redirect(reverse("projects:list"))

    project = _fetch_project(project_id)
    if not project:
        return HttpResponseBadRequest("Project not found")
    users = _fetch_users()
    coes = _get_all_coes()
    assigned_ids = _get_project_coe_ids(project_id)
    return render(request, "projects/edit_project.html", {
        "project": project, "users": users, "coes": coes, "assigned_coe_ids": assigned_ids
    })

@require_POST
def map_coes(request):
    """
    AJAX endpoint to map COEs to a project. Accepts:
      - project_choice: 'new' or existing project id
      - if 'new', also requires name (and optional description, start/end, pdl_username)
      - mapped_coe_ids: multiple values OK
    """
    project_choice = (request.POST.get("project_choice") or "").strip()
    selected_coes = request.POST.getlist("mapped_coe_ids")
    try:
        coe_ids = [int(x) for x in selected_coes if x]
    except Exception:
        coe_ids = []

    if project_choice == "new":
        name = (request.POST.get("name") or "").strip()
        if not name:
            return JsonResponse({"success": False, "error": "Project name required."}, status=400)
        desc = (request.POST.get("description") or "").strip()
        start_date = request.POST.get("start_date") or None
        end_date = request.POST.get("end_date") or None
        pdl_username = request.POST.get("pdl_username") or None
        pdl_user_id = None
        if pdl_username:
            pdl_user_id = _ensure_user_from_ldap(request.pdl_username)

        conn = get_connection()
        cur = conn.cursor()
        project_id = None
        try:
            cur.execute(
                "INSERT INTO projects (name, description, start_date, end_date, pdl_user_id) VALUES (%s, %s, %s, %s, %s)",
                (name, desc or None, start_date, end_date, pdl_user_id)
            )
            conn.commit()
            project_id = cur.lastrowid
        finally:
            cur.close(); conn.close()

        if project_id:
            _replace_project_coes(project_id, coe_ids)
        return JsonResponse({"success": True, "project_id": project_id})

    else:
        try:
            project_id = int(project_choice)
        except ValueError:
            return JsonResponse({"success": False, "error": "Invalid project selection."}, status=400)
        proj = _fetch_project(project_id)
        if not proj:
            return JsonResponse({"success": False, "error": "Project not found."}, status=404)
        _replace_project_coes(project_id, coe_ids)
        return JsonResponse({"success": True, "project_id": project_id})

@require_GET
def api_coes(request):
    coes = _get_all_coes()
    return JsonResponse({"coes": coes})

@require_GET
def api_projects(request):
    projects = _get_all_projects()
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT project_id, COUNT(*) as cnt FROM project_coes GROUP BY project_id")
        rows = cur.fetchall()
        counts = {r['project_id']: r['cnt'] for r in rows} if rows else {}
    finally:
        cur.close(); conn.close()
    for p in projects:
        p['mapped_coe_count'] = counts.get(p['id'], 0)
    return JsonResponse({"projects": projects})

def allocations_monthly(request):
    """
    Monthly allocations page for a PDL.
    Uses session ldap_username as canonical identity for PDL.
    """
    session_ldap = request.session.get("ldap_username")
    session_pwd = request.session.get("ldap_password")
    print("allocations_monthly - session_ldap:", session_ldap)
    from datetime import date
    # determine month_start
    month_str = request.GET.get("month")
    if month_str:
        try:
            month_start = datetime.strptime(month_str + "-01", "%Y-%m-%d").date()
        except Exception:
            month_start = date.today().replace(day=1)
    else:
        month_start = date.today().replace(day=1)

    # project selection
    project_id_param = request.GET.get("project_id")
    try:
        active_project_id = int(project_id_param) if project_id_param else 0
    except Exception:
        active_project_id = 0

    # fetch projects where this session ldap is PDL
    projects = []
    try:
        with connection.cursor() as cur:
            cur.execute("""
                SELECT p.id, p.name
                FROM projects p
                LEFT JOIN users u ON p.pdl_user_id = u.id
                WHERE u.ldap_id = %s
                ORDER BY p.name
            """, [session_ldap])
            projects = dictfetchall(cur)
    except Exception as exc:
        logger.exception("Error fetching projects for PDL: %s", exc)
        projects = []

    if not active_project_id and projects:
        active_project_id = projects[0].get("id", 0)

    if not active_project_id:
        return render(request, "projects/monthly_allocations.html", {
            "projects": projects,
            "active_project_id": active_project_id,
            "month_start": month_start,
            "coes": [],
            "domains_map": {},
            "allocation_map": {},
            "capacity_map": {},
            "hours_available": HOURS_AVAILABLE_PER_MONTH,
            "weekly_map": {},
        })

    # fetch all COEs (to show on right panel)
    try:
        with connection.cursor() as cur:
            cur.execute("SELECT id, name FROM coes ORDER BY name")
            coes = dictfetchall(cur)
    except Exception as exc:
        logger.exception("Error fetching COEs: %s", exc)
        coes = []

    coe_ids = [c["id"] for c in coes] if coes else []

    # domains grouped by coe
    domains_map = {}
    if coe_ids:
        try:
            with connection.cursor() as cur:
                cur.execute("SELECT id, name, coe_id FROM domains WHERE coe_id IN %s ORDER BY name", [tuple(coe_ids)])
                doms = dictfetchall(cur)
            for d in doms:
                domains_map.setdefault(d["coe_id"], []).append({"id": d["id"], "name": d["name"]})
        except Exception as exc:
            logger.exception("Error fetching domains: %s", exc)
            domains_map = {}

    # fetch allocation_items for this project/month
    allocation_map = {}
    capacity_accumulator = {}
    allocation_ids = []
    try:
        with connection.cursor() as cur:
            cur.execute("""
                SELECT ai.id AS item_id,
                       ai.allocation_id,
                       ai.coe_id,
                       ai.domain_id,
                       ai.user_ldap,
                       u.username AS username,
                       u.email AS email,
                       COALESCE(ai.total_hours,0) as total_hours
                FROM allocation_items ai
                JOIN allocations a ON ai.allocation_id = a.id
                LEFT JOIN users u ON ai.user_id = u.id
                WHERE ai.project_id = %s
                  AND a.month_start = %s
                ORDER BY ai.coe_id
            """, [active_project_id, month_start])
            items = dictfetchall(cur)

        for it in items:
            coe_id = it.get("coe_id") or 0
            ldap_val = (it.get("user_ldap") or "").strip()
            total_hours = int(it.get("total_hours") or 0)
            allocation_map.setdefault(coe_id, []).append({
                "item_id": it.get("item_id"),
                "allocation_id": it.get("allocation_id"),
                "coe_id": coe_id,
                "domain_id": it.get("domain_id"),
                "user_ldap": ldap_val,
                "username": it.get("username"),
                "email": it.get("email"),
                "total_hours": total_hours,
                "w1": 0, "w2": 0, "w3": 0, "w4": 0,
                "s1": "", "s2": "", "s3": "", "s4": ""
            })
            if ldap_val:
                key = ldap_val.lower()
                capacity_accumulator[key] = capacity_accumulator.get(key, 0) + total_hours
            aid = it.get("allocation_id")
            if aid and aid not in allocation_ids:
                allocation_ids.append(aid)
    except Exception as exc:
        logger.exception("Error fetching allocation_items: %s", exc)
        allocation_map = {}
        capacity_accumulator = {}
        allocation_ids = []

    # weekly allocations attach
    weekly_map = {}
    if allocation_ids:
        try:
            with connection.cursor() as cur:
                cur.execute("""
                    SELECT allocation_id, week_number, percent, status
                    FROM weekly_allocations
                    WHERE allocation_id IN %s
                """, [tuple(allocation_ids)])
                for r in dictfetchall(cur):
                    alloc = r["allocation_id"]
                    wk = int(r["week_number"])
                    weekly_map.setdefault(alloc, {})[wk] = {
                        "percent": float(r["percent"] or 0.0),
                        "status": (r.get("status") or "")
                    }
        except Exception as exc:
            logger.exception("Error fetching weekly_allocations: %s", exc)
            weekly_map = {}

        # attach
        for coe_id, items in allocation_map.items():
            for it in items:
                aid = it["allocation_id"]
                wk = weekly_map.get(aid, {})
                it["w1"] = wk.get(1, {}).get("percent", 0)
                it["w2"] = wk.get(2, {}).get("percent", 0)
                it["w3"] = wk.get(3, {}).get("percent", 0)
                it["w4"] = wk.get(4, {}).get("percent", 0)

                it["s1"] = wk.get(1, {}).get("status", "")
                it["s2"] = wk.get(2, {}).get("status", "")
                it["s3"] = wk.get(3, {}).get("status", "")
                it["s4"] = wk.get(4, {}).get("status", "")

    # capacity map
    capacity_map = {}
    for ldap_key, allocated in capacity_accumulator.items():
        remaining = max(0, HOURS_AVAILABLE_PER_MONTH - allocated)
        capacity_map[ldap_key] = {"allocated": allocated, "remaining": remaining}

    try:
        with connection.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT COALESCE(ai.user_ldap, '') as user_ldap
                FROM allocation_items ai
                JOIN allocations a ON ai.allocation_id = a.id
                WHERE ai.project_id = %s AND a.month_start = %s
            """, [active_project_id, month_start])
            for row in cur.fetchall():
                val = row[0] or ""
                key = val.strip().lower()
                if key and key not in capacity_map:
                    capacity_map[key] = {"allocated": 0, "remaining": HOURS_AVAILABLE_PER_MONTH}
    except Exception:
        pass

    return render(request, "projects/monthly_allocations.html", {
        "projects": projects,
        "active_project_id": active_project_id,
        "month_start": month_start,
        "coes": coes,
        "domains_map": domains_map,
        "allocation_map": allocation_map,
        "capacity_map": capacity_map,
        "hours_available": HOURS_AVAILABLE_PER_MONTH,
        "weekly_map": weekly_map,
    })
# -------------------------
# save_monthly_allocations
# -------------------------
@require_POST
def save_monthly_allocations(request):
    """
    Save monthly allocations payload from UI.

    Enhancements:
      - Canonicalize incoming user identifiers to LDAP login (userPrincipalName / mail)
      - Use request.session['ldap_username'] as canonical session identity
      - When necessary, resolve short samAccountName via local users table or via LDAP lookup
      - Always store users.username and users.ldap_id = canonical_login
    """
    session_ldap = request.session.get("ldap_username")
    session_pwd = request.session.get("ldap_password")
    logger.debug("save_monthly_allocations - session_ldap: %s", session_ldap)

    try:
        payload = json.loads(request.body.decode('utf-8'))
    except Exception:
        return HttpResponseBadRequest("Invalid JSON")

    project_id = payload.get("project_id")
    month_start = payload.get("month_start")
    items = payload.get("items", [])

    if not project_id or not month_start:
        return HttpResponseBadRequest("project_id and month_start are required")

    try:
        month_date = datetime.strptime(month_start, "%Y-%m-%d").date()
    except Exception:
        return HttpResponseBadRequest("month_start must be YYYY-MM-01")

    # Authorization: only PDL for project (by ldap_id) or admin
    with connection.cursor() as cur:
        cur.execute("SELECT pdl_user_id FROM projects WHERE id = %s LIMIT 1", [project_id])
        row = cur.fetchone()
        if not row:
            return HttpResponseBadRequest("Invalid project")
        pdl_user_id = row[0]
        cur.execute("SELECT id FROM users WHERE ldap_id = %s LIMIT 1", [session_ldap])
        urow = cur.fetchone()
        session_user_id = urow[0] if urow else None
        if pdl_user_id != session_user_id and request.user.username != 'admin':
            return HttpResponseForbidden("You are not authorized to save allocations for this project")

    # Helper to canonicalize an incoming ldap identifier (may be short samAccountName or canonical login)
    def _canonicalize_user_identifier(candidate: str):
        """
        Return canonical LDAP login (userPrincipalName or mail) for candidate.
        Strategy:
          1) If candidate already contains '@' -> assume canonical and return it.
          2) Check local users table: if users.ldap_id exists for users.username == candidate -> return users.ldap_id
          3) Attempt LDAP lookup using accounts.ldap_utils.get_user_entry_by_username(candidate, creds)
             - prefer attributes userPrincipalName, mail
          4) If all fails, return original candidate (but log warning)
        """
        if not candidate:
            return candidate
        cand = candidate.strip()
        if "@" in cand:
            return cand  # already canonical
        # 2) local DB mapping: if there is a users row with username = cand and ldap_id looks canonical, use it
        try:
            with connection.cursor() as cur:
                cur.execute("SELECT ldap_id FROM users WHERE username = %s LIMIT 1", [cand])
                r = cur.fetchone()
                if r and r[0]:
                    ldap_val = (r[0] or "").strip()
                    if ldap_val and "@" in ldap_val:
                        return ldap_val
        except Exception:
            logger.exception("Error checking local users mapping for %s", cand)

        # 3) LDAP lookup using credentials from session (if available)
        try:
            creds = (session_ldap, session_pwd) if session_ldap and session_pwd else None
            # get_user_entry_by_username should accept samAccountName or UPN
            user_entry = None
            try:
                user_entry = get_user_entry_by_username(cand, username_password_for_conn=creds)
            except Exception:
                logger.exception("LDAP lookup failed for %s", cand)
                user_entry = None

            if user_entry:
                # try to get userPrincipalName or mail attribute from LDAP entry
                # Note: entries may be ldap3 objects, dicts, or similar - handle both cases
                upn = None
                mail = None
                try:
                    # if ldap3 Entry: attributes accessible via .entry_attributes_as_dict or .get
                    if hasattr(user_entry, "entry_attributes_as_dict"):
                        attrs = user_entry.entry_attributes_as_dict
                        upn = attrs.get("userPrincipalName") or attrs.get("mail")
                        # attrs may be lists; take first
                        if isinstance(upn, (list, tuple)):
                            upn = upn[0] if upn else None
                    else:
                        # assume dict-like
                        upn = user_entry.get("userPrincipalName") or user_entry.get("mail")
                        if isinstance(upn, (list, tuple)):
                            upn = upn[0] if upn else None
                except Exception:
                    logger.exception("Error extracting attributes from LDAP entry for %s", cand)
                    upn = None

                if upn:
                    upn_val = upn.strip()
                    if upn_val:
                        return upn_val
        except Exception:
            logger.exception("LDAP resolution error for %s", cand)

        # fallback - return original (legacy short name); caller may still insert it but we log warning
        logger.warning("Could not canonicalize '%s' to UPN/mail; storing as-is (legacy).", cand)
        return cand

    # canonicalize items list in memory first
    canonical_items = []
    for it in items:
        # preserve original structure but canonicalize user_ldap if present
        coe_id = it.get("coe_id")
        domain_id = it.get("domain_id")
        total_hours = it.get("total_hours") or 0
        raw_user = (it.get("user_ldap") or "").strip()
        canonical_user = _canonicalize_user_identifier(raw_user) if raw_user else ""
        canonical_items.append({
            "coe_id": coe_id,
            "domain_id": domain_id,
            "user_ldap": canonical_user,
            "total_hours": int(total_hours or 0)
        })

    # Now proceed to upsert using canonical_items
    try:
        with transaction.atomic():
            ldap_to_alloc_id = {}
            unique_ldaps = sorted({ci["user_ldap"] for ci in canonical_items if ci.get("user_ldap")})
            for ldap in unique_ldaps:
                if not ldap:
                    continue
                with connection.cursor() as cur:
                    # ensure user exists with ldap_id = canonical ldap
                    cur.execute("SELECT id, ldap_id FROM users WHERE ldap_id = %s LIMIT 1", [ldap])
                    u = cur.fetchone()
                    if u:
                        user_id = u[0]
                    else:
                        # create user record with username=ldap and ldap_id=ldap
                        cur.execute(
                            "INSERT INTO users (username, ldap_id, role, created_at) VALUES (%s, %s, %s, CURRENT_TIMESTAMP)",
                            [ldap, ldap, 'EMPLOYEE']
                        )
                        user_id = cur.lastrowid

                    # upsert allocation (one per user/project/month)
                    cur.execute("SELECT id FROM allocations WHERE user_id = %s AND project_id = %s AND month_start = %s LIMIT 1",
                                [user_id, project_id, month_date])
                    a = cur.fetchone()
                    if a:
                        alloc_id = a[0]
                    else:
                        cur.execute("INSERT INTO allocations (user_id, project_id, month_start, total_hours, pending_hours) VALUES (%s, %s, %s, 0, 0)",
                                    [user_id, project_id, month_date])
                        alloc_id = cur.lastrowid
                    ldap_to_alloc_id[ldap] = (alloc_id, user_id)

            # Now upsert allocation_items using canonical ldap->alloc mapping
            incoming_keys = set()
            for it in canonical_items:
                ldap = it.get("user_ldap")
                if not ldap:
                    continue
                mapping = ldap_to_alloc_id.get(ldap)
                if not mapping:
                    # unexpected; skip
                    continue
                alloc_id, user_id = mapping
                coe_id = int(it.get("coe_id")) if it.get("coe_id") else None
                domain_id = int(it.get("domain_id")) if it.get("domain_id") else None
                hours = int(it.get("total_hours") or 0)

                if not coe_id:
                    continue

                incoming_keys.add((alloc_id, coe_id, user_id))

                with connection.cursor() as cur:
                    cur.execute("""
                        SELECT id FROM allocation_items
                        WHERE allocation_id = %s AND coe_id = %s AND user_id = %s LIMIT 1
                    """, [alloc_id, coe_id, user_id])
                    ex = cur.fetchone()
                    if ex:
                        item_id = ex[0]
                        cur.execute("""
                            UPDATE allocation_items
                            SET domain_id = %s, total_hours = %s, user_ldap = %s, updated_at = CURRENT_TIMESTAMP
                            WHERE id = %s
                        """, [domain_id, hours, ldap, item_id])
                    else:
                        cur.execute("""
                            INSERT INTO allocation_items (allocation_id, project_id, coe_id, domain_id, user_id, user_ldap, total_hours)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, [alloc_id, project_id, coe_id, domain_id, user_id, ldap, hours])

            # Delete allocation_items for this project+month not present in incoming_keys
            with connection.cursor() as cur:
                cur.execute("""
                    SELECT ai.id, ai.allocation_id, ai.coe_id, ai.user_id
                    FROM allocation_items ai
                    JOIN allocations a ON ai.allocation_id = a.id
                    WHERE ai.project_id = %s AND a.month_start = %s
                """, [project_id, month_date])
                existing_items = cur.fetchall()
                for item in existing_items:
                    ai_id, alloc_id, coe_id, user_id = item
                    if (alloc_id, coe_id, user_id) not in incoming_keys:
                        cur.execute("DELETE FROM allocation_items WHERE id = %s", [ai_id])

            # Recompute allocation.total_hours per allocation
            with connection.cursor() as cur:
                cur.execute("SELECT a.id FROM allocations a WHERE a.project_id = %s AND a.month_start = %s", [project_id, month_date])
                alloc_rows = cur.fetchall()
                for ar in alloc_rows:
                    aid = ar[0]
                    cur.execute("SELECT COALESCE(SUM(total_hours),0) FROM allocation_items WHERE allocation_id = %s", [aid])
                    total = cur.fetchone()[0] or 0
                    cur.execute("UPDATE allocations SET total_hours = %s, pending_hours = %s WHERE id = %s", [total, total, aid])

        return JsonResponse({"ok": True, "message": "Allocations saved with canonical ldap identifiers."})
    except Exception as exc:
        logger.exception("save_monthly_allocations failed: %s", exc)
        return JsonResponse({"ok": False, "error": str(exc)}, status=500)

# -------------------------
# team_allocations
# -------------------------
# ---- Helper utilities ---------------------------------------------------

def _sql_in_clause(items):
    """
    Return (sql_fragment, params_list) for an IN clause for psycopg/MySQL paramstyle (%s).
    If items is empty returns ("(NULL)", []) to produce a false IN clause safely.
    """
    if not items:
        return "(NULL)", []
    placeholders = ",".join(["%s"] * len(items))
    return f"({placeholders})", list(items)


def is_pdl_user(ldap_entry):
    """
    Determine whether the LDAP user is a PDL (Project Delivery Lead) or manager.
    This is a conservative check and should be replaced/extended based on your LDAP schema:
      - check memberOf for specific group
      - check 'title', 'employeeType', or a custom attr like 'role'
    ldap_entry is expected to be the object returned by get_user_entry_by_username.
    """
    if not ldap_entry:
        return False

    # try a few common attributes (adjust to your environment)
    try:
        # If your LDAP helper returns a dict-like or attribute accessor, adapt accordingly
        attrs = {}
        if hasattr(ldap_entry, "entry_attributes_as_dict"):
            attrs = ldap_entry.entry_attributes_as_dict
        elif isinstance(ldap_entry, dict):
            attrs = ldap_entry
        else:
            # fallback: try to access attribute names directly
            # create attrs by reading typical attr names if present
            for name in ("title", "employeeType", "memberOf", "role"):
                val = getattr(ldap_entry, name, None)
                if val:
                    attrs[name] = val

        # If explicit role attribute mentions PDL/Manager
        role_val = (attrs.get("employeeType") or attrs.get("title") or attrs.get("role") or "")
        if isinstance(role_val, (list, tuple)):
            role_val = " ".join(role_val)
        if role_val and ("pdl" in role_val.lower() or "project delivery" in role_val.lower() or "manager" in role_val.lower()):
            return True

        # If memberOf contains a PDL/Managers group
        member_of = attrs.get("memberOf") or attrs.get("memberof") or []
        if isinstance(member_of, str):
            member_of = [member_of]
        for grp in member_of:
            if "pdl" in grp.lower() or "manager" in grp.lower() or "project-delivery" in grp.lower():
                return True
    except Exception:
        logger.exception("is_pdl_user: unexpected structure for ldap_entry")

    return False


# ---- Main view ---------------------------------------------------------

def team_allocations(request):
    """
    Team Allocation page for manager/PDL:
    - lists allocations for direct reportees (from LDAP using session creds)
    - also includes the manager's own allocation rows if manager is PDL
    - attaches weekly allocations (w1..w4 + status) to each row
    """
    # SINGLE source of truth for LDAP username/password from session
    session_ldap = request.session.get("ldap_username")
    session_pwd = request.session.get("ldap_password")
    print("team_allocations - session_ldap: ", session_ldap)

    # require login
    if not session_ldap or not session_pwd:
        return redirect("accounts:login")
    creds = (session_ldap, session_pwd)
    from datetime import date
    # --- month_start param parsing ------------------------------------------------
    month_str = request.GET.get("month")
    if month_str:
        try:
            month_start = datetime.strptime(month_str + "-01", "%Y-%m-%d").date()
        except Exception:
            logger.exception("team_allocations: invalid month param '%s'", month_str)
            month_start = date.today().replace(day=1)
    else:
        month_start = date.today().replace(day=1)

    # --- get LDAP user entry -----------------------------------------------------
    user_entry = get_user_entry_by_username(session_ldap, username_password_for_conn=creds)
    if not user_entry:
        print("team_allocations: user_entry not found for :", session_ldap)
        return redirect("accounts:login")

    # --- get reportees via LDAP --------------------------------------------------
    reportees_entries = get_reportees_for_user_dn(getattr(user_entry, "entry_dn", None),
                                                 username_password_for_conn=creds) or []
    print("team_allocations: found %d reportees for %s", len(reportees_entries), session_ldap)
    # canonicalize reportees to login identifiers: prefer userPrincipalName then mail
    reportees_ldaps = []
    for ent in reportees_entries:
        # ent may be dict-like or LDAP entry object: try both access patterns
        val = None
        if isinstance(ent, dict):
            val = ent.get("userPrincipalName") or ent.get("mail") or ent.get("userid") or ent.get("sAMAccountName")
        else:
            # object-like: try attribute access
            for attr in ("userPrincipalName", "mail", "sAMAccountName", "uid"):
                val = getattr(ent, attr, None) or val
        if val:
            try:
                reportees_ldaps.append(val.strip())
            except Exception:
                reportees_ldaps.append(str(val))

    # If logged in user is PDL, include them as well (single source: session_ldap)
    try:
        if is_pdl_user(user_entry):
            if session_ldap not in reportees_ldaps:
                reportees_ldaps.append(session_ldap)
                logger.debug("team_allocations: user is PDL, added own ldap to reportees list")
    except Exception:
        logger.exception("team_allocations: error checking PDL role for %s", session_ldap)

    print("team_allocations: reportees_ldaps :", reportees_ldaps)

    # --- fetch allocation rows for these reportees for the selected month -----------
    rows = []
    if reportees_ldaps:
        in_clause, in_params = _sql_in_clause(reportees_ldaps)
        sql = f"""
            SELECT ai.id as item_id, ai.allocation_id, ai.user_ldap,
                   u.username, u.email,
                   p.name as project_name,
                   d.name as domain_name,
                   a.total_hours
            FROM allocation_items ai
            JOIN allocations a ON ai.allocation_id = a.id
            LEFT JOIN users u ON ai.user_id = u.id
            JOIN projects p ON ai.project_id = p.id
            LEFT JOIN domains d ON ai.domain_id = d.id
            WHERE a.month_start = %s
              AND ai.user_ldap IN {in_clause}
            ORDER BY u.username, p.name
        """
        params = [month_start] + in_params
        try:
            with connection.cursor() as cur:
                cur.execute(sql, params)
                rows = dictfetchall(cur) or []
                print("team_allocations: fetched %d allocation rows", len(rows))
        except Exception as exc:
            logger.exception("team_allocations: DB query failed: %s", exc)
            rows = []
    else:
        print("team_allocations: no reportees to fetch for %s", session_ldap)

    # --- merge/deduplicate (if needed) -------------------------------------------
    # If rows may contain duplicates based on your data model, dedupe by item_id
    dedup = {}
    for r in rows:
        key = r.get("item_id") or (r.get("allocation_id"), r.get("user_ldap"), r.get("project_name"))
        if key not in dedup:
            dedup[key] = r
    all_rows = list(dedup.values())

    # --- fetch weekly allocations for all allocation_ids --------------------------------
    allocation_ids = list({r["allocation_id"] for r in all_rows if r.get("allocation_id")})
    weekly_map = {}
    if allocation_ids:
        in_clause, in_params = _sql_in_clause(allocation_ids)
        sql = f"""
            SELECT allocation_id, week_number, hours, status
            FROM weekly_allocations
            WHERE allocation_id IN {in_clause}
        """
        try:
            with connection.cursor() as cur:
                cur.execute(sql, in_params)
                print("team_allocations: fetched weekly allocations for %d allocation_ids", len(allocation_ids))
                for r in dictfetchall(cur) or []:
                    try:
                        alloc = r.get("allocation_id")
                        wk = int(r.get("week_number") or 0)
                        hours = int(r.get("hours") or 0)
                        status = r.get("status") or ""
                        if alloc is None or wk <= 0:
                            continue
                        weekly_map.setdefault(alloc, {})[wk] = {"hours": hours, "status": status}
                    except Exception:
                        logger.exception("team_allocations: bad weekly row %r", r)
        except Exception as exc:
            logger.exception("team_allocations: weekly allocations query failed: %s", exc)
            weekly_map = {}
    print("team_allocations: weekly_map keys:", list(weekly_map.keys()))
    # --- attach weekly attrs and compute display_name --------------------------------
    for r in all_rows:
        aid = r.get("allocation_id")
        wk = weekly_map.get(aid, {})
        r["w1"] = wk.get(1, {}).get("hours", 0)
        r["w2"] = wk.get(2, {}).get("hours", 0)
        r["w3"] = wk.get(3, {}).get("hours", 0)
        r["w4"] = wk.get(4, {}).get("hours", 0)
        r["s1"] = wk.get(1, {}).get("status", "")
        r["s2"] = wk.get(2, {}).get("status", "")
        r["s3"] = wk.get(3, {}).get("status", "")
        r["s4"] = wk.get(4, {}).get("status", "")

        display = (r.get("username") or r.get("user_ldap") or "")
        if r.get("email"):
            display += f" <{r['email']}>"
        r["display_name"] = display

    # debug prints (optional; rely on logger in production)
    print("team_allocations: all_rows count=%d", len(all_rows))
    print("team_allocations: weekly_map keys=%s", list(weekly_map.keys()))
    #print context variables for debugging


    print("all rows :",all_rows)
    print("all weekly_map :",weekly_map)

    return render(request, "projects/team_allocations.html", {
        "rows": all_rows,
        "weekly_map": weekly_map,
        "month_start": month_start,
        "reportees": reportees_ldaps,
    })

# -------------------------
# save_team_allocation
# -------------------------
@require_POST
def save_team_allocation(request):
    """
    Save weekly % -> hours mapping for a given allocation_id.
    Validates that the session user is manager of the allocation (the user being updated must be a reportee).
    """
    session_ldap = request.session.get("ldap_username")
    session_pwd = request.session.get("ldap_password")
    print("save_team_allocation - session_ldap:", session_ldap)

    try:
        payload = json.loads(request.body.decode("utf-8"))
    except Exception:
        return HttpResponseBadRequest("Invalid JSON")

    allocation_id = payload.get("allocation_id")
    weekly = payload.get("weekly", {})

    if not allocation_id:
        return HttpResponseBadRequest("Missing allocation_id")

    # fetch allocation and user_ldap + total_hours
    with connection.cursor() as cur:
        cur.execute("""
            SELECT a.id, a.total_hours, ai.user_ldap
            FROM allocations a
            JOIN allocation_items ai ON ai.allocation_id = a.id
            WHERE a.id = %s LIMIT 1
        """, [allocation_id])
        rec = cur.fetchone()
        if not rec:
            return HttpResponseBadRequest("Invalid allocation_id")
        alloc_id, total_hours, user_ldap = rec

    # validate manager -> reportee via ldap_utils using session creds
    if not session_ldap or not session_pwd:
        return HttpResponseForbidden("Missing session LDAP credentials")

    creds = (session_ldap, session_pwd)
    user_entry = get_user_entry_by_username(session_ldap, username_password_for_conn=creds)
    if not user_entry:
        return HttpResponseForbidden("Manager not found in LDAP")

    user_dn = getattr(user_entry, "entry_dn", None)
    reportees_entries = get_reportees_for_user_dn(user_dn, username_password_for_conn=creds) or []
    reportees_ldaps = []
    for ent in reportees_entries:
        val = ent.get("userPrincipalName") or ent.get("mail")
        if val:
            reportees_ldaps.append(val)
    logger.debug("save_team_allocation: reportees_ldaps = %s", reportees_ldaps)


    # upsert weekly allocations as hours calculated from percentage
    try:
        with transaction.atomic():
            for week_str, pct in weekly.items():
                try:
                    week_num = int(week_str)
                    pct_val = float(pct)
                except Exception:
                    continue
                pct_val = max(0.0, min(100.0, pct_val))
                hours = int(round((total_hours or 0) * (pct_val / 100.0)))
                with connection.cursor() as cur:
                    cur.execute("""
                        INSERT INTO weekly_allocations (allocation_id, week_number, hours)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE hours = VALUES(hours), updated_at = CURRENT_TIMESTAMP
                    """, [allocation_id, week_num, hours])
        return JsonResponse({"ok": True})
    except Exception as exc:
        logger.exception("save_team_allocation failed: %s", exc)
        return JsonResponse({"ok": False, "error": str(exc)}, status=500)
# -------------------------
# my_allocations
# -------------------------
def my_allocations(request):
    """
    My Allocations view for logged-in user. Uses session ldap string as canonical filter.
    Shows weekly allocation hours and status fields.
    """
    session_ldap = request.session.get("ldap_username")
    print("my_allocations - session_ldap:", session_ldap)
    if not session_ldap:
        return redirect("accounts:login")
    from datetime import date
    month_str = request.GET.get("month")
    if month_str:
        try:
            month_start = datetime.strptime(month_str + "-01", "%Y-%m-%d").date()
        except Exception:
            month_start = date.today().replace(day=1)
    else:
        month_start = date.today().replace(day=1)

    rows = []
    allocation_ids = []
    try:
        with connection.cursor() as cur:
            cur.execute("""
                SELECT ai.id AS item_id,
                       ai.allocation_id,
                       ai.project_id,
                       p.name AS project_name,
                       ai.coe_id,
                       coe.name AS coe_name,
                       ai.domain_id,
                       d.name AS domain_name,
                       COALESCE(ai.total_hours,0) AS total_hours,
                       COALESCE(ai.user_ldap, '') AS user_ldap,
                       u.username AS username,
                       u.email AS email
                FROM allocation_items ai
                JOIN allocations a ON ai.allocation_id = a.id
                LEFT JOIN users u ON ai.user_id = u.id
                LEFT JOIN projects p ON ai.project_id = p.id
                LEFT JOIN domains d ON ai.domain_id = d.id
                LEFT JOIN coes coe ON ai.coe_id = coe.id
                WHERE a.month_start = %s
                  AND ai.user_ldap = %s
                ORDER BY p.name, coe.name
            """, [month_start, session_ldap])
            rows = dictfetchall(cur)
            allocation_ids = list({r["allocation_id"] for r in rows if r.get("allocation_id")})
    except Exception as exc:
        logger.exception("my_allocations fetch failed: %s", exc)
        rows = []
        allocation_ids = []

    # weekly map
    weekly_map = {}
    if allocation_ids:
        try:
            with connection.cursor() as cur:
                cur.execute("""
                    SELECT allocation_id, week_number, hours, status
                    FROM weekly_allocations
                    WHERE allocation_id IN %s
                """, [tuple(allocation_ids)])
                for w in dictfetchall(cur):
                    alloc = w["allocation_id"]
                    wknum = int(w["week_number"] or 0)
                    weekly_map.setdefault(alloc, {})[wknum] = {"hours": int(w["hours"] or 0), "status": w.get("status") or ""}
        except Exception as exc:
            logger.exception("my_allocations weekly fetch failed: %s", exc)
            weekly_map = {}

        # attach
        for r in rows:
            a_id = r.get("allocation_id")
            wk = weekly_map.get(a_id, {})
            r["w1"] = wk.get(1, {}).get("hours", 0)
            r["w2"] = wk.get(2, {}).get("hours", 0)
            r["w3"] = wk.get(3, {}).get("hours", 0)
            r["w4"] = wk.get(4, {}).get("hours", 0)
            r["s1"] = wk.get(1, {}).get("status", "")
            r["s2"] = wk.get(2, {}).get("status", "")
            r["s3"] = wk.get(3, {}).get("status", "")
            r["s4"] = wk.get(4, {}).get("status", "")

    # enrich display name
    for r in rows:
        display = (r.get("username") or r.get("user_ldap") or "")
        if r.get("email"):
            display += f" <{r['email']}>"
        r["display_name"] = display

    # group by project for template convenience
    grouped = {}
    for r in rows:
        proj = r.get("project_name") or "Other"
        grouped.setdefault(proj, []).append(r)

    return render(request, "projects/my_allocations.html", {
        "rows": rows,
        "grouped_rows": grouped,
        "weekly_map": weekly_map,
        "month_start": month_start,
        "ldap_username": session_ldap,
    })
# -------------------------
# my_allocations_update_status
# -------------------------
@require_POST
def my_allocations_update_status(request):
    """
    Update status (ACCEPTED/REJECTED) for weeks for the logged-in user's allocation.
    """
    session_ldap = request.session.get("ldap_username")
    print("my_allocations_update_status - session_ldap:", session_ldap)
    if not session_ldap:
        return HttpResponseForbidden("Missing LDAP session username")

    try:
        payload = json.loads(request.body.decode("utf-8"))
    except Exception:
        return HttpResponseBadRequest("Invalid JSON")

    allocation_id = payload.get("allocation_id")
    updates = payload.get("updates", {})

    if not allocation_id or not isinstance(updates, dict):
        return HttpResponseBadRequest("allocation_id and updates required")

    # verify allocation belongs to logged in user
    with connection.cursor() as cur:
        cur.execute("""
            SELECT a.id, ai.user_ldap, a.total_hours
            FROM allocations a
            JOIN allocation_items ai ON ai.allocation_id = a.id
            WHERE a.id = %s LIMIT 1
        """, [allocation_id])
        rec = cur.fetchone()
        if not rec:
            return HttpResponseBadRequest("Invalid allocation_id")
        db_alloc_id, db_user_ldap, total_hours = rec

    if (db_user_ldap or "").strip() != (session_ldap or "").strip():
        return HttpResponseForbidden("You are not authorized to update this allocation")

    try:
        with transaction.atomic():
            for week_str, action in updates.items():
                try:
                    week_num = int(week_str)
                except Exception:
                    continue
                act = (action or "").strip().upper()
                if act not in ("ACCEPT", "ACCEPTED", "REJECT", "REJECTED"):
                    continue
                status_val = "ACCEPTED" if act.startswith("ACCE") else "REJECTED"
                with connection.cursor() as cur:
                    cur.execute("SELECT hours FROM weekly_allocations WHERE allocation_id = %s AND week_number = %s LIMIT 1",
                                [allocation_id, week_num])
                    hh = cur.fetchone()
                    hours_val = int(hh[0]) if hh and hh[0] is not None else 0
                    cur.execute("""
                        INSERT INTO weekly_allocations (allocation_id, week_number, hours, status)
                        VALUES (%s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE hours = VALUES(hours), status = VALUES(status), updated_at = CURRENT_TIMESTAMP
                    """, [allocation_id, week_num, hours_val, status_val])
        return JsonResponse({"ok": True})
    except Exception as exc:
        logger.exception("my_allocations_update_status failed: %s", exc)
        return JsonResponse({"ok": False, "error": str(exc)}, status=500)

