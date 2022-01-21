from . import views
from django.urls import path

urlpatterns = [
    path("backtest/",views.backtestView,name="backtest")
]