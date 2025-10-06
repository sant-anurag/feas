from ldap3 import Server, Connection, ALL, SUBTREE
from django.conf import settings
import logging

logger = logging.getLogger(__name__)


def build_bind_username(input_username: str):
    """Build bind username and search filter."""
    if '@' in input_username:
        search_filter = f'(userPrincipalName={input_username})'
        return input_username, search_filter
    else:
        domain = getattr(settings, "LDAP_DOMAIN_PREFIX", None)
        bind_user = f"{domain}\\{input_username}" if domain else input_username
        search_filter = f'(sAMAccountName={input_username})'
        return bind_user, search_filter


def _get_ldap_connection(username: str = None, password: str = None):
    """Return an ldap3.Connection bound as user (if creds provided) or service account."""
    server_uri = getattr(settings, "LDAP_SERVER", None)
    if not server_uri:
        raise RuntimeError("LDAP_SERVER not configured in settings.")
    print(f"Connecting to LDAP server at {server_uri}")
    server_port = int(getattr(settings, "LDAP_PORT", 389))
    server = Server(server_uri, port=server_port, get_info=ALL)

    # Bind as user
    if username and password is not None:
        bind_user, _ = build_bind_username(username)
        conn = Connection(server, user=bind_user, password=password, receive_timeout=20, auto_bind=True)
        return conn

    # Fallback: service account
    bind_dn = getattr(settings, "LDAP_BIND_DN", None)
    bind_pw = getattr(settings, "LDAP_BIND_PASSWORD", None)
    print(f"Binding as service account: {bind_dn}")
    if bind_dn and bind_pw:
        conn = Connection(server, user=bind_dn, password=bind_pw, receive_timeout=20, auto_bind=True)
        return conn

    raise RuntimeError("No LDAP credentials provided.")


def get_user_entry_by_username(username: str, conn: Connection = None, username_password_for_conn: tuple = None):
    """Return LDAP entry for username (ldap3.Entry) or None."""
    close_conn = False
    if conn is None:
        if username_password_for_conn:
            u, p = username_password_for_conn
            conn = _get_ldap_connection(username=u, password=p)
        else:
            conn = _get_ldap_connection()
        close_conn = True

    user_search_base = getattr(settings, "LDAP_USER_SEARCH_BASE", "")
    base_dn = getattr(settings, "LDAP_BASE_DN", "")
    search_base = f"{user_search_base},{base_dn}" if user_search_base else base_dn

    if '@' in username:
        search_filter = f"(userPrincipalName={username})"
    else:
        search_filter = f"(sAMAccountName={username})"

    attributes = getattr(settings, "LDAP_ATTRIBUTES", [
        'cn', 'sAMAccountName', 'userPrincipalName', 'mail', 'department',
        'title', 'telephoneNumber', 'lastLogonTimestamp', 'memberOf', 'jpegPhoto',
        'manager', 'directReports'
    ])

    conn.search(search_base=search_base, search_filter=search_filter, search_scope=SUBTREE, attributes=attributes)
    entry = conn.entries[0] if conn.entries else None
    print(f"LDAP search for {username} returned: {entry}")
    if close_conn:
        conn.unbind()
    return entry


def get_reportees_for_user_dn(user_dn: str, conn: Connection = None, username_password_for_conn: tuple = None):
    """Return list of reportees for a given manager DN."""
    close_conn = False
    reportees = []
    print(f"Getting reportees from {user_dn}")
    if conn is None:
        if username_password_for_conn:
            u, p = username_password_for_conn
            conn = _get_ldap_connection(username=u, password=p)
        else:
            conn = _get_ldap_connection()
        close_conn = True

    attrs = getattr(settings, "LDAP_ATTRIBUTES", ["cn", "sAMAccountName", "mail", "title", "department", "manager"])

    # Try directReports
    conn.search(search_base=user_dn, search_filter="(objectClass=*)", search_scope='BASE', attributes=['directReports'])
    if conn.entries and hasattr(conn.entries[0], 'directReports') and conn.entries[0].directReports:
        drs = list(conn.entries[0].directReports.values)
        for rep_dn in drs:
            conn.search(search_base=rep_dn, search_filter='(objectClass=*)', search_scope='BASE', attributes=attrs)
            if conn.entries:
                e = conn.entries[0]
                reportees.append({
                    "dn": e.entry_dn,
                    "cn": str(getattr(e, 'cn', '')),
                    "sAMAccountName": str(getattr(e, 'sAMAccountName', '')),
                    "mail": str(getattr(e, 'mail', '')),
                    "title": str(getattr(e, 'title', '')),
                    "department": str(getattr(e, 'department', '')),
                })
    else:
        # Fallback: search by manager
        base_dn = getattr(settings, "LDAP_BASE_DN", "")
        search_filter = f"(manager={user_dn})"
        conn.search(search_base=base_dn, search_filter=search_filter, search_scope=SUBTREE, attributes=attrs)
        for e in conn.entries:
            reportees.append({
                "dn": e.entry_dn,
                "cn": str(getattr(e, 'cn', '')),
                "sAMAccountName": str(getattr(e, 'sAMAccountName', '')),
                "mail": str(getattr(e, 'mail', '')),
                "title": str(getattr(e, 'title', '')),
                "department": str(getattr(e, 'department', '')),
            })

    if close_conn:
        conn.unbind()
    return reportees
