from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    # path('admin/', admin.site.urls),
    path("api/backtest/",include("comet_backtester.urls")),
    path("api/analysis/",include("comet_analysis.urls"))
]
