# feas_project/urls.py
from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),

    # Root -> login (accounts app handles GET / -> login form)
    path('', include('accounts.urls', namespace='accounts')),

    # Dashboard under /dashboard/
    path('dashboard/', include('dashboard.urls', namespace='dashboard')),

    path('projects/', include('projects.urls', namespace='projects')),
    path('resources/', include('resources.urls')),
    path('settings/', include('settings.urls', namespace='settings')),
]
