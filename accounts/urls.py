# accounts/urls.py
from django.urls import path
from . import views
from django.shortcuts import redirect

app_name = "accounts"

urlpatterns = [
    path('login/', views.login_view, name='login'),
    path('logout/', views.logout_view, name='logout'),  # implement logout_view or use simple lambda
]
