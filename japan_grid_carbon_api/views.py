from django.shortcuts import render

# Create your views here.
from django.http import HttpResponse


def home(request):
    return HttpResponse("Hello There, this will be an API for Calculating the Carbon Intensity of the Japanese Grid!")
