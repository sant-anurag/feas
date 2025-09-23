# resources/urls.py
from django.urls import path
from . import views

app_name = "resources"

urlpatterns = [
    path("", views.redirect_to_directory, name="home"),
    path("directory/", views.employee_directory, name="directory"),
    path("directory/search/", views.ldap_local_search_api, name="ldap_local_search"),
    path("directory/profile/<int:ld_id>/", views.ldap_local_profile_api, name="ldap_local_profile"),
    path("ldap-sync/", views.ldap_sync_page, name="ldap_sync"),
    path("ldap-sync/start/", views.ldap_sync_start, name="ldap_sync_start"),
    path("ldap-sync/progress/", views.ldap_sync_progress, name="ldap_sync_progress"),
]
