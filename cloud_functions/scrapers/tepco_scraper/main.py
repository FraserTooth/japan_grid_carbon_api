from tepco_scraper import generateTEPCOJSON


def tepco_scraper(request):
    return (generateTEPCOJSON(), 200, {'Content-Type': 'application/json'})
