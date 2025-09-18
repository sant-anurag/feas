# projects/views.py
import logging
from django.shortcuts import render, redirect
from django.conf import settings
from django.urls import reverse
from django.http import JsonResponse, HttpResponseBadRequest, HttpResponseNotAllowed
from django.views.decorators.http import require_GET, require_POST
import mysql.connector
from mysql.connector import Error, IntegrityError

logger = logging.getLogger(__name__)


# -------------------- DB Connection helper --------------------
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


# -------------------- small helpers --------------------
def _ensure_user_from_ldap(samaccountname):
    """
    Ensure a user row exists for the provided sAMAccountName / username.
    Returns the local users.id.
    (If you have an LDAP utility available, integrate that here.)
    """
    if not samaccountname:
        return None
    sam = samaccountname.strip()

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT id FROM users WHERE ldap_id = %s OR username = %s LIMIT 1", (sam, sam))
        row = cur.fetchone()
        if row:
            return row["id"]
        # Insert minimal user record
        ins = conn.cursor()
        ins.execute("INSERT INTO users (username, ldap_id) VALUES (%s, %s)", (sam, sam))
        conn.commit()
        user_id = ins.lastrowid
        ins.close()
        return user_id
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


# -------------------- Project views --------------------
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


def create_project(request):
    """
    GET: render create-project page with coes/domains/users lists populated.
    POST: create the project.
    """
    if request.method == "POST":
        name = (request.POST.get("name") or "").strip()
        desc = (request.POST.get("description") or "").strip()
        start_date = request.POST.get("start_date") or None
        end_date = request.POST.get("end_date") or None
        pdl_username = request.POST.get("pdl_username") or None

        if not name:
            # re-render with error
            users = _fetch_users()
            conn = get_connection()
            cur = conn.cursor(dictionary=True)
            try:
                cur.execute("SELECT id, name FROM coes ORDER BY name")
                coes = cur.fetchall()
                cur.execute("SELECT id, name, coe_id FROM domains ORDER BY name")
                domains = cur.fetchall()
            finally:
                cur.close(); conn.close()
            return render(request, "projects/create_project.html", {
                "users": users,
                "coes": coes,
                "domains": domains,
                "error": "Project name is required."
            })

        pdl_user_id = None
        if pdl_username:
            pdl_user_id = _ensure_user_from_ldap(pdl_username)

        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "INSERT INTO projects (name, description, start_date, end_date, pdl_user_id) VALUES (%s, %s, %s, %s, %s)",
                (name, desc or None, start_date, end_date, pdl_user_id)
            )
            conn.commit()
        finally:
            cur.close(); conn.close()

        if request.headers.get("x-requested-with") == "XMLHttpRequest":
            return JsonResponse({"success": True})
        return redirect(request.META.get("HTTP_REFERER", reverse("projects:list")))

    # GET -> render page and populate selects
    users = _fetch_users()
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT id, name FROM coes ORDER BY name")
        coes = cur.fetchall()
        cur.execute("SELECT id, name, coe_id FROM domains ORDER BY name")
        domains = cur.fetchall()
    finally:
        cur.close(); conn.close()

    return render(request, "projects/create_project.html", {
        "users": users,
        "coes": coes,
        "domains": domains
    })


def edit_project(request, project_id):
    if request.method == "POST":
        name = (request.POST.get("name") or "").strip()
        desc = (request.POST.get("description") or "").strip()
        start_date = request.POST.get("start_date") or None
        end_date = request.POST.get("end_date") or None
        pdl_username = request.POST.get("pdl_username") or None

        if not name:
            return HttpResponseBadRequest("Project name required")

        pdl_user_id = None
        if pdl_username:
            pdl_user_id = _ensure_user_from_ldap(pdl_username)

        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                UPDATE projects SET name=%s, description=%s, start_date=%s, end_date=%s, pdl_user_id=%s WHERE id=%s
            """, (name, desc or None, start_date, end_date, pdl_user_id, project_id))
            conn.commit()
        finally:
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
    try:
        cur.execute("DELETE FROM projects WHERE id=%s", (project_id,))
        conn.commit()
    finally:
        cur.close(); conn.close()
    return redirect(reverse("projects:list"))


# -------------------- COE views (idempotent & robust) --------------------
@require_POST
def create_coe(request):
    name = (request.POST.get("name") or "").strip()
    leader_username = request.POST.get("leader_username") or None
    description = request.POST.get("description") or None

    if not name:
        return HttpResponseBadRequest("COE name required")

    # existence check
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
        leader_user_id = _ensure_user_from_ldap(leader_username)

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
        leader_user_id = _ensure_user_from_ldap(leader_username)

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


# -------------------- Domain views (idempotent & robust) --------------------
@require_POST
def create_domain(request):
    name = (request.POST.get("name") or "").strip()
    coe_id = request.POST.get("coe_id") or None
    lead_username = request.POST.get("lead_username") or None

    if not name:
        return HttpResponseBadRequest("Domain name required")

    # normalize coe_id
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
        lead_user_id = _ensure_user_from_ldap(lead_username)

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
        lead_user_id = _ensure_user_from_ldap(lead_username)

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


# -------------------- LDAP search endpoint --------------------

# -------------------- LDAP search endpoint --------------------
@require_GET
def ldap_search(request):
    q = (request.GET.get("q") or "").strip()
    if len(q) < 1:
        return JsonResponse({"results": []})

    # Try to use LDAP if ldap_utils available
    results = []
    try:
        from accounts import ldap_utils
        username =  request.session.get("ldap_username")
        password =  request.session.get("ldap_password")
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
        # fallback to local users table
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
