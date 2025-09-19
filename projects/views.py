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

logger = logging.getLogger(__name__)

HOURS_AVAILABLE_PER_MONTH = 183.75

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


def _get_project_coe_ids(project_id):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT coe_id FROM project_coes WHERE project_id=%s", (project_id,))
        rows = cur.fetchall()
        return [r['coe_id'] for r in rows] if rows else []
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


def create_project(request):
    """
    GET: render create-project page with coes/domains/users lists populated.
    POST: create the project and assign selected COEs (mapped_coe_ids[]).
    """
    if request.method == "POST":
        name = (request.POST.get("name") or "").strip()
        desc = (request.POST.get("description") or "").strip()
        start_date = request.POST.get("start_date") or None
        end_date = request.POST.get("end_date") or None
        pdl_username = request.POST.get("pdl_username") or None

        # list of coe ids (may be multiple)
        mapped_coe_ids = request.POST.getlist("mapped_coe_ids")

        if not name:
            users = _fetch_users()
            coes = _get_all_coes()
            conn = get_connection()
            cur = conn.cursor(dictionary=True)
            try:
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

        # map coes (if any)
        try:
            int_coe_ids = [int(x) for x in mapped_coe_ids if x]
        except Exception:
            int_coe_ids = []
        if project_id and int_coe_ids:
            _assign_coes_to_project(project_id, int_coe_ids)

        if request.headers.get("x-requested-with") == "XMLHttpRequest":
            return JsonResponse({"success": True, "project_id": project_id})
        return redirect(reverse("projects:list"))

    # GET -> render
    users = _fetch_users()
    coes = _get_all_coes()
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
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
    """
    GET: render project edit page with assigned COEs checked.
    POST: update project fields and replace mapped COEs per form submission.
    """
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

        # replace COE mappings
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
        "project": project,
        "users": users,
        "coes": coes,
        "assigned_coe_ids": assigned_ids
    })


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

def _get_all_coes():
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT id, name FROM coes ORDER BY name")
        return cur.fetchall()
    finally:
        cur.close(); conn.close()

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

def _replace_project_coes(project_id, coe_ids):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM project_coes WHERE project_id=%s", (project_id,))
        if coe_ids:
            for cid in coe_ids:
                try:
                    cur.execute("INSERT INTO project_coes (project_id, coe_id) VALUES (%s, %s)", (project_id, cid))
                except IntegrityError:
                    # ignore duplicate/integrity errors for safety
                    continue
        conn.commit()
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

# helper to run queries returning dict rows
def dictfetchall(cursor):
    cols = [c[0] for c in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]

def allocations_monthly(request):
    print("Allocations monthly view called")
    """
    Page that shows monthly allocations tabs for projects where current user is PDL.
    GET params:
      - month: YYYY-MM (optional, defaults to current month)
      - project_id: active tab (optional)
    """
    username = request.session.get('ldap_username')
    print("Allocations monthly for user:", username)
    # month param => first day of month
    month_str = request.GET.get('month')
    if month_str:
        try:
            month_start = datetime.strptime(month_str + "-01", "%Y-%m-%d").date()
        except Exception:
            month_start = datetime.today().replace(day=1).date()
    else:
        month_start = datetime.today().replace(day=1).date()

    project_id_active = request.GET.get('project_id')

    # 1) find user row in users table
    with connection.cursor() as cur:
        cur.execute("SELECT id, username FROM users WHERE username = %s LIMIT 1", [username])
        me = cur.fetchone()
        me_id = me[0] if me else None

    # 2) find projects where pdl_user_id == me_id
    projects = []
    with connection.cursor() as cur:
        cur.execute("""
          SELECT id, name, start_date, end_date
          FROM projects
          WHERE pdl_user_id = %s
          ORDER BY name
        """, [me_id])
        projects = dictfetchall(cur)

    # pick active project if not provided
    if not project_id_active and projects:
        project_id_active = projects[0]['id']

    # 3) fetch COEs mapped to the active project (project_coes -> coes)
    coes = []
    domains = {}
    if project_id_active:
        with connection.cursor() as cur:
            cur.execute("""
              SELECT c.id, c.name
              FROM project_coes pc
              JOIN coes c ON pc.coe_id = c.id
              WHERE pc.project_id = %s
              ORDER BY c.name
            """, [project_id_active])
            coes = dictfetchall(cur)

            # get domains for the coe list
            coe_ids = [str(c['id']) for c in coes]
            if coe_ids:
                cur.execute(f"""
                  SELECT id, coe_id, name
                  FROM domains
                  WHERE coe_id IN ({','.join(['%s']*len(coe_ids))})
                  ORDER BY name
                """, coe_ids)
                rows = dictfetchall(cur)
                for r in rows:
                    domains.setdefault(r['coe_id'], []).append(r)

    # 4) fetch existing allocation_items and allocations for this project and month
    allocation_map = {}  # coe_id -> list of items
    if project_id_active:
        with connection.cursor() as cur:
            cur.execute("""
              SELECT ai.id as item_id, ai.coe_id, ai.domain_id, ai.user_id, ai.user_ldap, ai.total_hours,
                     u.username as username, d.name as domain_name
              FROM allocation_items ai
              LEFT JOIN users u ON ai.user_id = u.id
              LEFT JOIN domains d ON ai.domain_id = d.id
              WHERE ai.project_id = %s AND ai.allocation_id IN (
                SELECT id FROM allocations WHERE project_id = %s AND month_start = %s
              )
              ORDER BY ai.coe_id, u.username
            """, [project_id_active, project_id_active, month_start])
            rows = dictfetchall(cur)
            for r in rows:
                allocation_map.setdefault(r['coe_id'], []).append(r)

    # hours available per employee in month (business rule) - configurable; default 160
    hours_available = int(getattr(__import__('django.conf').conf.settings, 'HOURS_AVAILABLE_PER_MONTH', 160))

    context = {
        'projects': projects,
        'active_project_id': int(project_id_active) if project_id_active else None,
        'coes': coes,
        'domains_map': domains,
        'allocation_map': allocation_map,
        'month_start': month_start,
        'hours_available': hours_available,
    }
    return render(request, 'projects/monthly_allocations.html', context)


@require_http_methods(["POST"])
def save_monthly_allocations(request):
    """
    Accepts JSON payload:
    {
      "project_id": <int>,
      "month_start": "YYYY-MM-01",
      "items": [
        {"coe_id": 10, "domain_id": 21 or null, "user_ldap": "jdoe", "total_hours": 120},
        ...
      ]
    }

    Policy:
      - Only the project's PDL (matched via users.ldap_id OR auth_user.id) or admin can save.
      - Uses request.session.get("ldap_username") as canonical LDAP username.
    """
    try:
        payload = json.loads(request.body.decode('utf-8'))
    except Exception:
        return HttpResponseBadRequest("Invalid JSON")

    project_id = payload.get('project_id')
    month_start = payload.get('month_start')
    items = payload.get('items', [])

    if not project_id or not month_start:
        return HttpResponseBadRequest("project_id and month_start are required")

    try:
        month_date = datetime.strptime(month_start, "%Y-%m-%d").date()
    except Exception:
        return HttpResponseBadRequest("month_start must be YYYY-MM-01")

    # canonical ldap username from session
    session_ldap = request.session.get("ldap_username")
    auth_user_id = request.user.id
    # Sanity check: verify project exists and current user is PDL (via ldap mapping) or admin
    with connection.cursor() as cur:
        cur.execute("SELECT pdl_user_id FROM projects WHERE id = %s LIMIT 1", [project_id])
        row = cur.fetchone()
        if not row:
            return HttpResponseBadRequest("Invalid project")
        pdl_user_id = row[0]

        # Determine whether allowed:
        # - If pdl_user_id equals auth_user_id => allowed
        # - OR if pdl_user_id references users table where users.ldap_id == session_ldap => allowed
        allowed = False
        if pdl_user_id == auth_user_id:
            allowed = True
        else:
            if session_ldap:
                cur.execute("SELECT 1 FROM users WHERE id = %s AND (ldap_id = %s OR username = %s) LIMIT 1", [pdl_user_id, session_ldap, session_ldap])
                if cur.fetchone():
                    allowed = True

        # allow admin (username 'admin') for testing/backdoor as before
        if not allowed and request.user.username == 'admin':
            allowed = True

        if not allowed:
            return HttpResponseForbidden("You are not authorized to save allocations for this project")

    # transaction: upsert allocations and allocation_items
    try:
        with transaction.atomic():
            ldap_to_alloc_id = {}

            # Build unique list of ldap usernames from incoming items
            unique_ldaps = sorted({(it.get('user_ldap') or '').strip() for it in items if it.get('user_ldap')})
            for ldap in unique_ldaps:
                if not ldap:
                    continue
                # lookup user by ldap_id or username
                with connection.cursor() as cur:
                    cur.execute("SELECT id FROM users WHERE ldap_id = %s OR username = %s LIMIT 1", [ldap, ldap])
                    u = cur.fetchone()
                    if u:
                        user_id = u[0]
                    else:
                        # insert minimal user row, setting both username and ldap_id to ldap
                        cur.execute("INSERT INTO users (username, ldap_id, role, created_at) VALUES (%s, %s, %s, CURRENT_TIMESTAMP)", [ldap, ldap, 'EMPLOYEE'])
                        # fetch last inserted id
                        try:
                            user_id = cur.lastrowid
                        except Exception:
                            cur.execute("SELECT LAST_INSERT_ID()")
                            user_id = cur.fetchone()[0]

                    # ensure allocations row exists for this user/project/month
                    with connection.cursor() as cur2:
                        cur2.execute("SELECT id FROM allocations WHERE user_id = %s AND project_id = %s AND month_start = %s LIMIT 1", [user_id, project_id, month_date])
                        a = cur2.fetchone()
                        if a:
                            alloc_id = a[0]
                        else:
                            cur2.execute("INSERT INTO allocations (user_id, project_id, month_start, total_hours, pending_hours) VALUES (%s, %s, %s, 0, 0)", [user_id, project_id, month_date])
                            try:
                                alloc_id = cur2.lastrowid
                            except Exception:
                                cur2.execute("SELECT LAST_INSERT_ID()")
                                alloc_id = cur2.fetchone()[0]

                    ldap_to_alloc_id[ldap] = (alloc_id, user_id)

            # Upsert allocation_items
            incoming_keys = set()
            for it in items:
                ldap = (it.get('user_ldap') or '').strip()
                if not ldap:
                    continue
                alloc_tuple = ldap_to_alloc_id.get(ldap)
                if not alloc_tuple:
                    # skip items where we couldn't create/find a user
                    continue
                alloc_id, user_id = alloc_tuple
                try:
                    coe_id = int(it.get('coe_id')) if it.get('coe_id') else None
                except Exception:
                    coe_id = None
                try:
                    domain_id = int(it.get('domain_id')) if it.get('domain_id') else None
                except Exception:
                    domain_id = None
                try:
                    hours = int(it.get('total_hours') or 0)
                except Exception:
                    hours = 0

                if not coe_id:
                    continue

                incoming_keys.add((alloc_id, coe_id, user_id))

                with connection.cursor() as cur:
                    cur.execute("SELECT id FROM allocation_items WHERE allocation_id = %s AND coe_id = %s AND user_id = %s LIMIT 1", [alloc_id, coe_id, user_id])
                    existing = cur.fetchone()
                    if existing:
                        item_id = existing[0]
                        cur.execute("UPDATE allocation_items SET domain_id = %s, total_hours = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s", [domain_id, hours, item_id])
                    else:
                        cur.execute("INSERT INTO allocation_items (allocation_id, project_id, coe_id, domain_id, user_id, user_ldap, total_hours) VALUES (%s, %s, %s, %s, %s, %s, %s)", [alloc_id, project_id, coe_id, domain_id, user_id, ldap, hours])

            # Delete allocation_items not present in incoming_keys for this project+month
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

            # Recompute allocations.total_hours and pending_hours
            with connection.cursor() as cur:
                cur.execute("SELECT id FROM allocations WHERE project_id = %s AND month_start = %s", [project_id, month_date])
                alloc_rows = cur.fetchall()
                for ar in alloc_rows:
                    alloc_id = ar[0]
                    cur.execute("SELECT COALESCE(SUM(total_hours),0) FROM allocation_items WHERE allocation_id = %s", [alloc_id])
                    total = cur.fetchone()[0] or 0
                    cur.execute("UPDATE allocations SET total_hours = %s, pending_hours = %s WHERE id = %s", [total, total, alloc_id])

        return JsonResponse({"ok": True, "message": "Allocations saved."})
    except Exception as exc:
        return JsonResponse({"ok": False, "error": str(exc)}, status=500)

