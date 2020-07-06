import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import requests
import os
# from ..scraper import tepco_scraper
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "../scraper"))
from tepco_scraper import parseTepcoCsvs
import tepco_carbon_intensity as tci

dirname = os.path.dirname(__file__)
plotsFolder = os.path.join(dirname, '_output/plots/')
dataFolder = os.path.join(dirname, '_output/data/')


def _makePlots():
    ####################### Process ####################
    df = parseTepcoCsvs()
    df = tci.addCarbonIntensityFactors(df)

    # Allow Timebased Breakdowns against date facts
    times = pd.DatetimeIndex(df.datetime)

    dailyAverage, monthlyAverage = createDailyAndMonthlyAverageGroup(df, times)

    dailyAverageByMonth = createDailyAveragePerMonth(df, times)

    ##################### Plotting ####################

    print("Creating Plots")
    # Plot Year's Carbon Intensity
    plot = df.plot.line(x="datetime", y="carbon_intensity")
    fig = plot.get_figure()
    fig.savefig(os.path.join(plotsFolder, 'test.png'))

    # Plot Daily Carbon Intensity
    dailyPlot = dailyAverage.plot.line(y="carbon_intensity")
    dailyfig = dailyPlot.get_figure()
    dailyfig.savefig(os.path.join(
        plotsFolder, 'dailytest.png'))

    # Plot Average Carbon Intensity In Month
    monthlyPlot = monthlyAverage.plot.bar(y="carbon_intensity")
    monthlyPlot.set_ylim(
        monthlyAverage["carbon_intensity"].min() * 0.9, monthlyAverage["carbon_intensity"].max() * 1.1)
    monthlyfig = monthlyPlot.get_figure()
    monthlyfig.savefig(os.path.join(
        plotsFolder, 'monthlytest.png'))

    # Plot Daily Carbon Intensity for Each Month
    # NICE FORMATTING STUFF
    # These are the "Tableau 20" colors as RGB.
    tableau20 = [(31, 119, 180), (174, 199, 232), (255, 127, 14), (255, 187, 120),
                 (44, 160, 44), (152, 223, 138), (214, 39, 40), (255, 152, 150),
                 (148, 103, 189), (197, 176, 213), (140, 86, 75), (196, 156, 148),
                 (227, 119, 194), (247, 182, 210), (127, 127, 127), (199, 199, 199),
                 (188, 189, 34), (219, 219, 141), (23, 190, 207), (158, 218, 229)]

    # Scale the RGB values to the [0, 1] range, which is the format matplotlib accepts.
    for i in range(len(tableau20)):
        r, g, b = tableau20[i]
        tableau20[i] = (r / 255., g / 255., b / 255.)

    dailyMonthPlot = dailyAverageByMonth.plot.line(
        color=tableau20, title='Carbon Intensity in Tokyo over a Given Day, By Month (2016-Now)')

    dailyMonthPlot.set_xlabel('Hour')
    dailyMonthPlot.set_ylabel('CO2ge/kWh')
    dailyMonthfig = dailyMonthPlot.get_figure()
    dailyMonthfig.savefig(os.path.join(
        plotsFolder, 'dailyMonthTest.png'))


_makePlots()
