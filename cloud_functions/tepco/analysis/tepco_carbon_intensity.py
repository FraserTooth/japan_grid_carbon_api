from ..scrapers.tepco_scraper.tepco_scraper import parseTepcoCsvs
import os

dirname = os.path.dirname(__file__)
plotsFolder = os.path.join(dirname, '_output/plots/')
dataFolder = os.path.join(dirname, '_output/data/')


def addCarbonIntensityFactors():
    # Get And Calculate Carbon Intensity
    print("Grabbing Intensities")
    response = requests.get(
        "https://api.carbonintensity.org.uk/intensity/factors")

    # Thermal Data: https://www7.tepco.co.jp/fp/thermal-power/list-e.html
    fossilFuelStations = {
        "lng": 4.38+3.6+3.6+5.16+3.42+3.541+1.15+2+1.14,
        "oil": 5.66+1.05+4.40,
        "coal": 2
    }
    totalFossil = fossilFuelStations["lng"] + \
        fossilFuelStations["oil"]+fossilFuelStations["coal"]

    json = response.json()
    factors = json["data"][0]

    print("Resolving Intensities for Tokyo")
    # print(factors)
    carbonIntensity = {
        "kWh_nuclear": factors["Nuclear"],
        "kWh_fossil": (factors["Coal"]*fossilFuelStations["coal"] + factors["Oil"]*fossilFuelStations["oil"] + factors["Gas (Open Cycle)"]*fossilFuelStations["lng"])/totalFossil,
        "kWh_hydro": factors["Hydro"],
        "kWh_geothermal": 0,  # Probably
        "kWh_biomass": factors["Biomass"],
        "kWh_solar_output": factors["Solar"],
        "kWh_wind_output": factors["Wind"],
        "kWh_pumped_storage": factors["Pumped Storage"],
        # TODO: Replace this with a rolling calculation of the average of other parts of Japan's carbon intensity, probably around 850 though
        "kWh_interconnectors": 850
    }
    # print(carbonIntensity)
    return carbonIntensity


def carbonCalculation(row, carbonIntensity):

    # Reference: https://github.com/carbon-intensity/methodology/blob/master/Carbon%20Intensity%20Forecast%20Methodology.pdf
    nuclearIntensity = row["kWh_nuclear"] * carbonIntensity["kWh_nuclear"]
    fossilIntensity = row["kWh_fossil"] * carbonIntensity["kWh_fossil"]
    hydroIntensity = row["kWh_hydro"] * carbonIntensity["kWh_hydro"]
    geothermalIntensity = row["kWh_geothermal"] * \
        carbonIntensity["kWh_geothermal"]
    biomassIntensity = row["kWh_biomass"] * carbonIntensity["kWh_biomass"]
    solarIntensity = row["kWh_solar_output"] * \
        carbonIntensity["kWh_solar_output"]
    windIntensity = row["kWh_wind_output"] * carbonIntensity["kWh_wind_output"]
    pumpedStorageIntensity = row["kWh_pumped_storage"] * \
        carbonIntensity["kWh_pumped_storage"]
    interconnectorIntensity = row["kWh_interconnectors"] * \
        carbonIntensity["kWh_interconnectors"]

    return (nuclearIntensity + fossilIntensity + hydroIntensity + geothermalIntensity + biomassIntensity + solarIntensity + windIntensity + pumpedStorageIntensity + interconnectorIntensity) / row["kWh_total"]


def getTEPCODataframewithCarbonIntensity():
    df = parseTepcoCsvs()

    carbonIntensityFactors = addCarbonIntensityFactors()

    # Add Carbon Intensity
    print("Calculating Carbon Intensity")
    df["carbon_intensity"] = df.apply(
        lambda row: carbonCalculation(row, carbonIntensityFactors), axis=1)

    # Remove timezone
    # df["datetime"].dt.tz_localize("JST")

    return df


def createDailyAndMonthlyAverageGroup(df, times):
    # Grouping Functions
    print("Creating Grouped Data")

    # Create a Daily Average of Carbon Intensity Against the Time of Day for all days and years
    dailyAverage = df.groupby([times.hour]).mean()

    # Create a average of the Carbon Intensity for all times in a given month
    monthlyAverage = df.groupby([times.month]).mean()

    return dailyAverage, monthlyAverage


def createDailyAveragePerMonth(df, times):
    # Create a average of the Carbon Intensity for all times in a given month
    dailyAverageByMonth = pd.pivot_table(df, index=[times.hour], columns=[
        times.month], values="carbon_intensity", aggfunc=np.mean)
    dailyAverageByMonth.columns.name = "month"
    dailyAverageByMonth.index.name = "hour"
    return dailyAverageByMonth


def _makePlots():
    ####################### Process ####################
    df = parseTepcoCsvs()
    carbonIntensityFactors = addCarbonIntensityFactors()

    # Add Carbon Intensity
    print("Calculating Carbon Intensity")
    df["carbon_intensity"] = df.apply(
        lambda row: carbonCalculation(row, carbonIntensityFactors), axis=1)

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
        monthlyAverage["carbon_intensity"].min()*0.9, monthlyAverage["carbon_intensity"].max()*1.1)
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