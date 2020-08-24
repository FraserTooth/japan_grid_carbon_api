# japan_grid_carbon_api

API and Backend to Calculate the Current Carbon Intensity of the Japanese Grid...roughly

## Work so far:

- Pipeline for Extracting and Formatting TEPCO, Tohokuden and KEPCO Data
- Combined and extensible endpoint using Flask routing
- Added Pytest Tests
- Implementation of the [Serverless](https://www.serverless.com/) deployment framework
- Basic Website Built [Repo](https://github.com/FraserTooth/japan_grid_carbon_api_website)

## Planned Work:

- Make carbon intensity value data available in the form of an API, based on averaging historic data
- Combine Weather Data and Historic Data with ML to make forecast about 'likely current intensity'
- Allow historic query on predictions
- Incorporate other regions in Japan
- Build Good Documentation for the API

Data has been retrived from:

- TEPCO: [Source Link](http://www.tepco.co.jp/forecast/html/area_data-j.html)

Example Graphs So Far:

> ![Carbon Intensity in Tokyo over a Given Day, By Month (2016-Now)](misc/dailyMonthEarlyPlot.png)  
> _An Early Carbon Intensity Graph, this is the average amount of Carbon Emitted in Tokyo for every unit of electricity used, for all hours of the day, for each month of the year.  
> Derived from Public TEPCO Data_

### Useful Functions Snippets

```bash
# Open Virtual Environment
source .venv/bin/activate

# Read the Recent Logs
gcloud functions logs read

# Deploy Endpoint
./deployendpoint.sh daily_carbon_intensity

# Run Locally
./local.sh api main

# Deploy All
./deployAll.sh

# Run Tests
pytest -vv

# PIP Freeze with Line to Get Around pkg-resources==0.0.0 bug in Linux
pip freeze | grep -v "pkg-resources" > requirements.txt
```
### Links Used when calculating the Carbon Intensities in Japan

- https://www.fepc.or.jp/about_us/pr/pdf/kaiken_s_e_20170616.pdf
- https://e-lcs.jp/news/detail/000208.html
- https://www.jepic.or.jp/pub/pdf/epijJepic2019.pdf
- https://www.fepc.or.jp/english/library/electricity_eview_japan/__icsFiles/afieldfile/2016/08/24/2016ERJ_full.pdf

