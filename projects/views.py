# projects/views.py
from django.shortcuts import render, redirect
from django.conf import settings
from django.urls import reverse
from django.http import JsonResponse, HttpResponseBadRequest, HttpResponseNotAllowed
import mysql.connector
from mysql.connector import Error
from django.views.decorators.http import require_POST, require_GET
from django.views.decorators.csrf import csrf_exempt, csrf_protect
from django.utils.html import escape
from django.http import JsonResponse
from django.views.decorators.http import require_GET
import logging
logger = logging.getLogger(__name__)

# import LDAP helpers from your accounts app
from accounts import ldap_utils

def get_connection():
    dbs = settings.DATABASES["default"]
    return mysql.connector.connect(
        host=dbs.get("HOST", "127.0.0.1") or "127.0.0.1",
        port=int(dbs.get("PORT", 3306) or 3306),
        user=dbs.get("USER", "root") or "",
        password=dbs.get("PASSWORD", "root") or "",
        database=dbs.get("NAME", "feasdb") or "",
        charset="utf8mb4",
        use_unicode=True,
    )

# ---------------- Projects ----------------
def project_list(request):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT p.id, p.name, p.start_date, p.end_date, p.description,
               p.pdl_user_id, u.username as pdl_name
        FROM projects p
        LEFT JOIN users u ON p.pdl_user_id = u.id
        ORDER BY p.created_at DESC
    """)
    projects = cursor.fetchall()
    cursor.close(); conn.close()
    return render(request, "projects/project_list.html", {"projects": projects})

def create_project(request):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    if request.method == "POST":
        name = request.POST.get("name", "").strip()
        desc = request.POST.get("description", "").strip()
        start_date = request.POST.get("start_date") or None
        end_date = request.POST.get("end_date") or None
        pdl_username = request.POST.get("pdl_username") or None  # stores sAMAccountName from LDAP

        # map LDAP username to users table id (upsert into users if missing)
        pdl_user_id = None
        if pdl_username:
            pdl_user_id = _ensure_user_from_ldap(pdl_username)

        if not name:
            cursor.close(); conn.close()
            users = _fetch_users()
            # fetch coes for right-side select
            db = get_connection()
            c = db.cursor(dictionary=True)
            c.execute("SELECT id, name FROM coes ORDER BY name")
            coes = c.fetchall()
            c.close();
            db.close()
            return render(request, "projects/create_project.html", {"users": users, "coes": coes})

        cur = conn.cursor()
        cur.execute("""
            INSERT INTO projects (name, description, start_date, end_date, pdl_user_id)
            VALUES (%s, %s, %s, %s, %s)
        """, (name, desc, start_date, end_date, pdl_user_id))
        conn.commit()
        cur.close(); cursor.close(); conn.close()
        return redirect(reverse("projects:list"))

    users = _fetch_users()
    cursor.close(); conn.close()

    return render(request, "projects/create_project.html", {"users": users})


def edit_project(request, project_id):
    if request.method == "POST":
        # update
        name = request.POST.get("name", "").strip()
        desc = request.POST.get("description", "").strip()
        start_date = request.POST.get("start_date") or None
        end_date = request.POST.get("end_date") or None
        pdl_username = request.POST.get("pdl_username") or None

        pdl_user_id = None
        if pdl_username:
            pdl_user_id = _ensure_user_from_ldap(pdl_username)

        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE projects SET name=%s, description=%s, start_date=%s, end_date=%s, pdl_user_id=%s
            WHERE id=%s
        """, (name, desc, start_date, end_date, pdl_user_id, project_id))
        conn.commit()
        cur.close(); conn.close()
        return redirect(reverse("projects:list"))

    project = _fetch_project(project_id)
    if not project:
        return HttpResponseBadRequest("Project not found")
    users = _fetch_users()
    return render(request, "projects/edit_project.html", {"project": project, "users": users})


@require_POST
def delete_project(request, project_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM projects WHERE id = %s", (project_id,))
    conn.commit()
    cur.close(); conn.close()
    return redirect(reverse("projects:list"))


# ---------------- Users helper (LDAP sync into local users table) ----------------
def _ensure_user_from_ldap(samaccountname):
    """
    Given sAMAccountName from LDAP, ensure user exists in users table.
    Returns user.id (local DB).
    """
    # fetch entry from LDAP
    conn = ldap_utils._get_ldap_connection()  # uses service account configured in settings
    # build search filter
    filt = f"(|(sAMAccountName={samaccountname})(userPrincipalName={samaccountname}))"
    base_dn = getattr(settings, "LDAP_BASE_DN", "")
    conn.search(search_base=base_dn, search_filter=filt, search_scope='SUBTREE', attributes=['sAMAccountName','mail','cn'])
    if conn.entries:
        e = conn.entries[0]
        sAM = str(getattr(e, 'sAMAccountName', ''))
        mail = str(getattr(e, 'mail', '')) or None
        cn = str(getattr(e, 'cn', '')) or sAM
    else:
        # if not found, treat as plain username
        sAM = samaccountname
        mail = None
        cn = samaccountname

    conn.unbind()

    # upsert into users table based on sAMAccountName -> store in ldap_id or username
    db = get_connection()
    cur = db.cursor(dictionary=True)
    # try find by ldap_id or username
    cur.execute("SELECT id FROM users WHERE ldap_id = %s OR username = %s LIMIT 1", (sAM, sAM))
    row = cur.fetchone()
    if row:
        user_id = row['id']
    else:
        # insert
        ins = db.cursor()
        ins.execute("INSERT INTO users (username, email, ldap_id) VALUES (%s, %s, %s)", (sAM, mail, sAM))
        db.commit()
        user_id = ins.lastrowid
        ins.close()
    cur.close(); db.close()
    return user_id


def _fetch_users():
    db = get_connection()
    cur = db.cursor(dictionary=True)
    cur.execute("SELECT id, username, email FROM users ORDER BY username LIMIT 500")
    rows = cur.fetchall()
    cur.close(); db.close()
    return rows

def _fetch_project(project_id):
    db = get_connection()
    cur = db.cursor(dictionary=True)
    cur.execute("SELECT id, name, description, start_date, end_date, pdl_user_id FROM projects WHERE id = %s", (project_id,))
    row = cur.fetchone()
    cur.close(); db.close()
    return row

# ---------------- COEs and Domains ----------------
def create_coe(request):
    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])
    name = request.POST.get("name", "").strip()
    leader_username = request.POST.get("leader_username") or None  # sAMAccountName
    description = request.POST.get("description") or None

    leader_user_id = None
    if leader_username:
        leader_user_id = _ensure_user_from_ldap(leader_username)

    db = get_connection()
    cur = db.cursor()
    cur.execute("INSERT INTO coes (name, leader_user_id, description) VALUES (%s, %s, %s)", (name, leader_user_id, description))
    db.commit()
    cur.close(); db.close()
    return redirect(request.META.get('HTTP_REFERER', reverse("projects:create")))

def edit_coe(request, coe_id):
    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])
    name = request.POST.get("name", "").strip()
    leader_username = request.POST.get("leader_username") or None
    description = request.POST.get("description") or None

    leader_user_id = None
    if leader_username:
        leader_user_id = _ensure_user_from_ldap(leader_username)

    db = get_connection()
    cur = db.cursor()
    cur.execute("UPDATE coes SET name=%s, leader_user_id=%s, description=%s WHERE id=%s", (name, leader_user_id, description, coe_id))
    db.commit()
    cur.close(); db.close()
    return redirect(request.META.get('HTTP_REFERER', reverse("projects:create")))

def create_domain(request):
    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])
    name = request.POST.get("name", "").strip()
    coe_id = request.POST.get("coe_id")
    lead_username = request.POST.get("lead_username") or None
    lead_user_id = None
    if lead_username:
        lead_user_id = _ensure_user_from_ldap(lead_username)

    db = get_connection()
    cur = db.cursor()
    cur.execute("INSERT INTO domains (coe_id, name, lead_user_id) VALUES (%s, %s, %s)", (coe_id, name, lead_user_id))
    db.commit()
    cur.close(); db.close()
    return redirect(request.META.get('HTTP_REFERER', reverse("projects:create")))

def edit_domain(request, domain_id):
    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])
    name = request.POST.get("name", "").strip()
    coe_id = request.POST.get("coe_id")
    lead_username = request.POST.get("lead_username") or None
    lead_user_id = None
    if lead_username:
        lead_user_id = _ensure_user_from_ldap(lead_username)

    db = get_connection()
    cur = db.cursor()
    cur.execute("UPDATE domains SET coe_id=%s, name=%s, lead_user_id=%s WHERE id=%s", (coe_id, name, lead_user_id, domain_id))
    db.commit()
    cur.close(); db.close()
    return redirect(request.META.get('HTTP_REFERER', reverse("projects:create")))

# ---------------- LDAP Search AJAX ----------------

# projects/views.py (replace ldap_search with this)
@require_GET
def ldap_search(request):
    q = request.GET.get("q", "").strip()
    if len(q) < 3:
        return JsonResponse({"results": []})

    # Helper to acquire connection: try service account first, then session user creds, then anonymous
    conn = None
    try:
        # 1) Try service account via ldap_utils._get_ldap_connection() (no args)
        try:
            conn = ldap_utils._get_ldap_connection()   # existing function: binds service account if settings present
        except RuntimeError as rexc:
            logger.info("Service account not configured or failed: %s", rexc)

        # 2) If no service conn, try session-stored user creds (DEV / opt-in)
        if conn is None:
            ldap_user = request.session.get("ldap_username")
            ldap_pw = request.session.get("ldap_password")
            if ldap_user and ldap_pw:
                try:
                    conn = ldap_utils._get_ldap_connection(username=ldap_user, password=ldap_pw)
                    logger.info("LDAP search using session user credentials for %s", ldap_user)
                except Exception as e:
                    logger.exception("Failed to bind with session user creds")
        print("LDAP conn after service and session user attempts:", conn)
        # 3) If still None, attempt an anonymous bind if your LDAP allows it (optional)
        if conn is None:
            try:
                # If ldap_utils supports an anonymous bind, call it; else create a bare Connection
                conn = ldap_utils._get_ldap_connection(username=None, password=None)
            except Exception:
                # Some ldap wrappers raise RuntimeError if no creds — handle next by returning an informative error
                conn = None

        if conn is None:
            # No way to bind: return an informative error for the front-end (HTTP 503 or 500)
            logger.warning("LDAP search requested but no bind credentials available (service account or session user)")
            return JsonResponse({"error": "LDAP search unavailable: no bind credentials configured. Contact admin."}, status=503)

    except Exception as e:
        logger.exception("Unexpected error while obtaining LDAP connection")
        return JsonResponse({"error": "LDAP connection failed"}, status=500)

    # perform search
    try:
        q_escaped = q.replace('(', '\\28').replace(')', '\\29')
        base_dn = getattr(settings, "LDAP_BASE_DN", "")
        search_filter = f"(|(cn=*{q_escaped}*)(sAMAccountName=*{q_escaped}*)(mail=*{q_escaped}*))"
        attrs = ['cn', 'sAMAccountName', 'mail', 'title', 'department']

        # Try ldap3 style search signature; be tolerant to different wrappers
        try:
            conn.search(search_base=base_dn, search_filter=search_filter, search_scope='SUBTREE', attributes=attrs, size_limit=50)
            entries = conn.entries
        except TypeError:
            conn.search(base_dn, search_filter, attributes=attrs, size_limit=50)
            entries = conn.entries

        results = []
        for e in entries:
            results.append({
                "cn": str(getattr(e, 'cn', '') or ''),
                "sAMAccountName": str(getattr(e, 'sAMAccountName', '') or ''),
                "mail": str(getattr(e, 'mail', '') or ''),
                "title": str(getattr(e, 'title', '') or ''),
                "department": str(getattr(e, 'department', '') or ''),
            })
    except Exception as e:
        logger.exception("LDAP search failed")
        try:
            if hasattr(conn, "unbind"):
                conn.unbind()
            elif hasattr(conn, "close"):
                conn.close()
        except Exception:
            pass
        return JsonResponse({"error": "LDAP search failed"}, status=500)

    # close connection gracefully
    try:
        if hasattr(conn, "unbind"):
            conn.unbind()
        elif hasattr(conn, "close"):
            conn.close()
    except Exception:
        pass

    return JsonResponse({"results": results})

