from django.urls import path
from . import views

app_name = "settings"

urlpatterns = [
    path("import-master/", views.import_master, name="import_master"),
    path("monthly_hours/", views.monthly_hours_settings, name="monthly_hours_settings"),
    path("save_monthly_hours/", views.save_monthly_hours, name="save_monthly_hours"),
    path("get_monthly_max/", views.get_monthly_max, name="get_monthly_max"),
]
