from urllib.request import urlopen
import re


class UtilityAreaScraper:
    def get_data_urls_from_page(self, page_url, csv_link_regex_pattern, csv_base_url, charset="utf-8"):
        page = urlopen(page_url)
        html_bytes = page.read()
        html = html_bytes.decode(charset)

        partial_urls = re.findall(csv_link_regex_pattern, html)

        return map(lambda u: csv_base_url + u, partial_urls)
