# feas_project/urls.py
from django.contrib import admin
from django.urls import path, include
from django.shortcuts import redirect
from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', lambda req: redirect('accounts:login')),   # root -> login
    path('accounts/', include(('accounts.urls', 'accounts'), namespace='accounts')),
    # other apps...
]

# In dev, serve static files (only when DEBUG=True)
if settings.DEBUG:
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATICFILES_DIRS[0])
