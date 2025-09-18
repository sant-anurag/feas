# accounts/context_processors.py
"""
Expose a role-based menu to all templates.

It reads role from request.session['role'] (set during login).
If absent, defaults to 'EMPLOYEE'.
"""

from django.urls import reverse_lazy

# canonical menu tree. Each item can have: title, icon (optional), url (name or raw),
# submenus = list of same shape, roles = list of roles allowed to see this menu.
MENU_TREE = [
    {
        "key": "dashboard",
        "title": "Dashboard",
        "url": reverse_lazy("dashboard:home"),
        "roles": ["ADMIN", "PDL", "COE_LEADER", "TEAM_LEAD", "EMPLOYEE"],
    },
    {
        "key": "projects",
        "title": "Projects",
        "url": "#",
        "roles": ["ADMIN", "PDL", "COE_LEADER", "TEAM_LEAD"],
        "submenus": [
            {"key": "projects_list", "title": "Projects List", "url": reverse_lazy("projects:list"),"roles": ["ADMIN", "PDL"]},
            {"key": "create_project", "title": "Create Project", "url": reverse_lazy("projects:create"),"roles": ["ADMIN", "PDL"]},
        ],
    },

    {
        "key": "resources",
        "title": "Resource Management",
        "url": "#",
        "roles": ["ADMIN", "PDL", "COE_LEADER", "TEAM_LEAD"],
        "submenus": [
            {"key": "directory", "title": "Employee Directory", "url": "#", "roles": ["ADMIN","PDL","COE_LEADER","TEAM_LEAD"]},
            {"key": "ldap_sync", "title": "Import / Sync LDAP", "url": "#", "roles": ["ADMIN"]},
        ],
    },
    {
        "key": "allocations",
        "title": "Allocations",
        "url": "#",
        "roles": ["ADMIN", "PDL", "COE_LEADER", "TEAM_LEAD", "EMPLOYEE"],
        "submenus": [
            {"key": "monthly", "title": "Monthly Allocation", "url": "#", "roles": ["PDL","ADMIN"]},
            {"key": "weekly", "title": "Weekly Allocation", "url": "#", "roles": ["COE_LEADER","TEAM_LEAD","ADMIN"]},
            {"key": "my_alloc", "title": "My Allocations", "url": "#", "roles": ["EMPLOYEE"]},
        ],
    },
    {
        "key": "coes",
        "title": "COE & Domains",
        "url": "#",
        "roles": ["ADMIN","COE_LEADER","TEAM_LEAD"],
        "submenus": [
            {"key": "coe_list", "title": "COE List", "url": "#", "roles": ["ADMIN","COE_LEADER"]},
            {"key": "add_coe", "title": "Add / Edit COE", "url": "#", "roles": ["ADMIN"]},
        ],
    },
    {
        "key": "reports",
        "title": "Reports & Analytics",
        "url": "#",
        "roles": ["ADMIN", "PDL", "COE_LEADER", "TEAM_LEAD"],
        "submenus": [
            {"key": "util", "title": "Utilization", "url": "#", "roles": ["ADMIN","PDL","COE_LEADER","TEAM_LEAD"]},
            {"key": "custom", "title": "Custom Report", "url": "#", "roles": ["ADMIN"]},
        ],
    },
    {
        "key": "settings",
        "title": "Settings",
        "url": "#",
        "roles": ["ADMIN"],
        "submenus": [
            {"key": "ldap", "title": "LDAP Configuration", "url": "#", "roles": ["ADMIN"]},
            {"key": "system", "title": "System Config", "url": "#", "roles": ["ADMIN"]},
        ],
    },
    {
        "key": "admin",
        "title": "Admin",
        "url": "/admin/",
        "roles": ["ADMIN"],
    },
]

# utility to filter menu by role
def _filter_menu_by_role(menu_tree, role):
    visible = []
    for item in menu_tree:
        roles = item.get("roles", [])
        if role in roles:
            item_copy = item.copy()
            # process submenus if present
            sub = item.get("submenus")
            if sub:
                filtered_sub = []
                for s in sub:
                    if role in s.get("roles", []):
                        filtered_sub.append(s.copy())
                if filtered_sub:
                    item_copy["submenus"] = filtered_sub
                else:
                    item_copy.pop("submenus", None)
            visible.append(item_copy)
    return visible

def menu_processor(request):
    """
    Context processor that adds 'feas_menu' (list) and 'feas_user_role' to templates.
    Assumes role is stored in request.session['role'] (set in login_view).
    """
    role = request.session.get("role", "EMPLOYEE")  # default
    # normalize role to uppercase
    role = role.upper() if isinstance(role, str) else "EMPLOYEE"
    menu = _filter_menu_by_role(MENU_TREE, role)
    return {
        "feas_menu": menu,
        "feas_user_role": role,
    }
