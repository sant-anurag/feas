from django.urls import path
from . import views

app_name = 'dashboard'

urlpatterns = [
    # === MAIN DASHBOARD PAGE ===
    path('', views.dashboard_view, name='home'),

    # === PDL endpoints (hours-only focused) ===
    # Monthly series (consumed vs estimated). Query param style: /api/pdl_hours_series/?year=2025
    path('api/pdl_hours_series/', views.pdl_hours_series, name='pdl_hours_series'),
    path('api/pdl_hours_series/<int:year>/', views.pdl_hours_series, name='pdl_hours_series_year'),

    # Program / department breakdown (YTD or single month).
    # Query params: ?year=YYYY (&month=1..12 optionally) (&dept=DeptName optionally)
    path('api/pdl_program_breakdown/', views.pdl_program_breakdown, name='pdl_program_breakdown'),
    path('api/pdl_program_breakdown/<int:year>/', views.pdl_program_breakdown, name='pdl_program_breakdown_year'),

    # Department summary (donut/pie) - used to populate dept select (compatibility)
    path('api/pdl_dept_summary/<int:year>/', views.pdl_dept_summary, name='pdl_dept_summary'),
]
