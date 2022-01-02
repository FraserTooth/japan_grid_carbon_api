from urllib.request import urlopen
import pandas as pd
import re
from func_timeout import func_timeout, FunctionTimedOut


class UtilityAreaScraper:
    def get_data_urls_from_page(self, page_url: str, csv_link_regex_pattern, csv_base_url: str, charset: str ="utf-8"):
        page = urlopen(page_url)
        html_bytes = page.read()
        html = html_bytes.decode(charset)

        partial_urls = re.findall(csv_link_regex_pattern, html)

        return map(lambda u: csv_base_url + u, partial_urls)

    def _get_file(self, url: str, **kwargs):
        return pd.read_csv(
            url,
            **kwargs["scrapeKwargs"]
        )

    def get_csv_with_timeout(self, url: str, **kwargs):
        try:
            data = func_timeout(5, self._get_file, kwargs={"url": url, **kwargs})
        except FunctionTimedOut as e:
            print(f"Timeout on {url} - retrying...")
            return self.get_csv_with_timeout(url, **kwargs)
        return data
