# japan_grid_carbon_api

API and Backend to Calculate the Current Carbon Intensity of the Japanese Grid...roughly

## Work so far:
- Pipeline for Extracting and Formatting TEPCO Data

## Planned Work:
- Make carbon intensity value data available in the form of an API, based on averaging historic data
- Combine Weather Data and Historic Data with ML to make forecast about 'likely current intensity'
- Allow historic query on predictions
- Incorporate other regions in Japan

Data has been retrived from:

- TEPCO: [Source Link](http://www.tepco.co.jp/forecast/html/area_data-j.html)

Example Graphs So Far:  
>![Carbon Intensity in Tokyo over a Given Day, By Month (2016-Now)](misc/dailyMonthEarlyPlot.png)  
>_An Early Carbon Intensity Graph, this is the average amount of Carbon Emitted in Tokyo for every unit of electricity used, for all hours of the day, for each month of the year.  
> Derived from Public TEPCO Data_

### Useful Functions Snippets
```
gcloud functions logs read
```