# base/views.py
from django.shortcuts import render, redirect
def dashboard_view(request):
    if not request.session.get('is_authenticated'):
        return redirect('accounts:login')
    return render(request, "base.html")
