# accounts/urls.py
from django.urls import path
from . import views

app_name = "accounts"

urlpatterns = [
    path('', views.login_view, name='login'),           # GET  /  -> login form
    path('login/', views.login_view, name='login_alt'), # GET  /login/ -> same view (optional)
    path('logout/', views.logout_view, name='logout'),
]
