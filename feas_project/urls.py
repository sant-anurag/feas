from django.contrib import admin
from django.urls import path, include
from django.shortcuts import redirect

urlpatterns = [
    path('admin/', admin.site.urls),
    path('accounts/', include('accounts.urls', namespace='accounts')),
    path('', include('base.urls', namespace='dashboard')),  # landing dashboard
    path("projects/", include("projects.urls", namespace="projects")),
path("resources/", include("resources.urls")),
path("settings/", include("settings.urls", namespace="settings")),



]

