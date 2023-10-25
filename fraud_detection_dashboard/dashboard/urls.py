from django.urls import path
from .views import kpi_view

urlpatterns = [
    path('kpi/', kpi_view, name='kpi'),
]