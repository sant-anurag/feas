# projects/urls.py
from django.urls import path
from . import views

app_name = "projects"

urlpatterns = [
    path("list/", views.project_list, name="list"),
    path("create/", views.create_project, name="create"),
    path("edit/<int:project_id>/", views.edit_project, name="edit"),
    path("delete/<int:project_id>/", views.delete_project, name="delete"),

    # COE & Domain management (raw-sql)
    path("coes/create/", views.create_coe, name="coes_create"),
    path("coes/edit/<int:coe_id>/", views.edit_coe, name="coes_edit"),
    path("domains/create/", views.create_domain, name="domains_create"),
    path("domains/edit/<int:domain_id>/", views.edit_domain, name="domains_edit"),

    # AJAX endpoints
    path("ldap-search/", views.ldap_search, name="ldap_search"),

    # New endpoints required by the mapping UI and live-refresh:
    path("map-coes/", views.map_coes, name="map_coes"),
    path("api/coes/", views.api_coes, name="api_coes"),
    path("api/projects/", views.api_projects, name="api_projects"),
]
