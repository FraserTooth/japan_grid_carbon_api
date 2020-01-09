from django.urls import path
from japan_grid_carbon_api import views

urlpatterns = [
    path("", views.home, name="home"),
]
