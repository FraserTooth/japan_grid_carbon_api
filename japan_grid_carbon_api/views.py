from django.contrib.auth.models import User, Group
from japan_grid_carbon_api.serializers import UserSerializer, GroupSerializer
from rest_framework import viewsets
from django.shortcuts import render

# Create your views here.
from django.http import HttpResponse


def home(request):
    return HttpResponse("Hello There, this will be an API for Calculating the Carbon Intensity of the Japanese Grid!")


class UserViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows users to be viewed or edited.
    """
    queryset = User.objects.all().order_by('-date_joined')
    serializer_class = UserSerializer


class GroupViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows groups to be viewed or edited.
    """
    queryset = Group.objects.all()
    serializer_class = GroupSerializer
