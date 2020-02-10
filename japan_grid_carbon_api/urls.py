from japan_grid_carbon_api import views
from rest_framework import routers
from django.urls import include, path
from django.urls import path
from japan_grid_carbon_api import views

# urlpatterns = [
#     path("", views.home, name="home"),
# ]


router = routers.DefaultRouter()
router.register(r'users', views.UserViewSet)
router.register(r'groups', views.GroupViewSet)

# Wire up our API using automatic URL routing.
# Additionally, we include login URLs for the browsable API.
urlpatterns = [
    path("", views.home, name="home"),
    path('api/', include(router.urls)),
    path('api-auth/', include('rest_framework.urls', namespace='rest_framework'))
]
