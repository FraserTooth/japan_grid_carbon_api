from django.db import models

# Create your models here.


class TEPCOEnergyData(models.Model):
    date = models.DateField()
    time = models.TimeField()
    datetime = models.DateTimeField()
    kWh_demand = models.IntegerField()
    kWh_nuclear = models.IntegerField()
    kWh_fossil = models.IntegerField()
    kWh_hydro = models.IntegerField()
    kWh_geothermal = models.IntegerField()
    kWh_biomass = models.IntegerField()
    kWh_solar_output = models.IntegerField()
    kWh_solar_throttling = models.IntegerField()
    kWh_wind_output = models.IntegerField()
    kWh_wind_throttling = models.IntegerField()
    kWh_pumped_storage = models.IntegerField()
    kWh_interconnectors = models.IntegerField()
    kWh_total = models.IntegerField()
    kWh_total = models.IntegerField()
    carbon_intensity = models.DecimalField(
        max_digits=6, decimal_places=2, default=0)
