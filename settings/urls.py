from django.urls import path
from . import views

app_name = "settings"

urlpatterns = [
    path("import-master/", views.import_master, name="import_master"),
]
