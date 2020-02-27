from django.contrib.auth.models import User, Group
from japan_grid_carbon_api.models import TEPCOEnergyData
from rest_framework import serializers


class UserSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = User
        fields = ['url', 'username', 'email', 'groups']


class GroupSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = Group
        fields = ['url', 'name']


class TEPCODataSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = TEPCOEnergyData
        fields = [
            "date",
            "time",
            "datetime",
            "kWh_demand",
            "kWh_nuclear",
            "kWh_fossil",
            "kWh_hydro",
            "kWh_geothermal",
            "kWh_biomass",
            "kWh_solar_output",
            "kWh_solar_throttling",
            "kWh_wind_output",
            "kWh_wind_throttling",
            "kWh_pumped_storage",
            "kWh_interconnectors",
            "kWh_total",
            "kWh_total",
            "carbon_intensity"
        ]
