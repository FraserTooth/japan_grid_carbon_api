from tepco_scraper import generateTEPCOJSON, generateTEPCOCsv


def tepco_scraper(request):
    return (generateTEPCOCsv(), 200, {'Content-Type': 'text/csv'})
