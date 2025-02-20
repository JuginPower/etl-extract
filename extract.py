import requests
from requests.auth import HTTPBasicAuth
from datalayer import SqliteDatamanager
from settings import webbrowsers, delay, datapi_url, datapi_passwd, datapi_username
from random import choice
from time import sleep
from bs4 import BeautifulSoup
import re
from datetime import datetime
import logging
import sys


logger = logging.getLogger(__name__)
logging.basicConfig(filename="extract.log", encoding="utf-8", level=logging.WARNING, format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d, %H:%M:%S')

class CrawlSpider(SqliteDatamanager):

    def __init__(self, domain_id: int):
        super().__init__()

        sql_string = f"""
        select url_name from urls u 
        join domains d on (u.domain_id=d.domain_id)
        where d.domain_id={domain_id} and d.active > 0 and u.active > 0;
        """
        self._domain_id = domain_id
        self._start_urls = [u[0] for u in self.select(sql_string)]

    def get_domain_id(self) -> int:
        """
        Getter for the domain id

        :return: A domain id which was specified by initializing.
        """
        return self._domain_id

    def get_domain_name(self) -> str:
        """
        Getter for the domain name.

        :return: The domain name for the given domain_id attribute
        """
        query = f"select domain_name from domains where domain_id={self.get_domain_id()};"

        try:
            domain_name = self.select(query)[0][0]
        except IndexError as err:
            logger.error(err)
        else:
            return domain_name

    def start_requests(self):
        """
        Starts requests for the given urls in the database and sends the data further to the writer method
        """

        for url in self._start_urls:
            sleep(delay)
            self.parse(requests.get(url, headers={'user-agent': choice(webbrowsers)}))

    def send_data(self, line: dict) -> int:
        """
        Sends the data to the api from the pipline.

        :param line: A dict line which will be sending
        :return: A status code which identify if it was successfully or not.
        """

        response = requests.post(datapi_url + "transform", json=line, auth=HTTPBasicAuth(datapi_username, datapi_passwd))
        return response.status_code

    def parse(self, response: requests.Response) -> dict:

        pass


class StockSpider1(CrawlSpider):

    def __init__(self, domain_id: int):
        super().__init__(domain_id)

    def parse(self, response: requests.Response) -> int:

        soup = BeautifulSoup(response.content, "html.parser")

        title: str = soup.find("title").getText().split("|")[0].strip()

        css_selectors = [".quote", ".instrument-info"]

        payload = None

        for selector in css_selectors:
            selected = soup.css.select_one(selector)

            if selected:
                data = re.search(r"\d+,\d+|\d+\.\d{2,},\d+", str(selected))
                url_name = response.url
                category = self.select(f"select category from urls where url_name='{url_name}';")[0][0]

                payload = {"title": title,
                           "data": data.group(0),
                           "datum": datetime.now().strftime("%Y-%m-%d, %H:%M:%S"),
                           "category": category,
                           "domain_name": self.get_domain_name(),
                           "url_name": url_name
                           }

        self.send_data(payload)

def send(domain_id: int):

    match domain_id:
        case 1:
            stock_spider = StockSpider1(domain_id)
            stock_spider.start_requests()
        case _:
            logger.error(f"There is no domain for domain_id {domain_id}")


if __name__ == '__main__':

    stock_spider = StockSpider1(1)
    stock_spider.start_requests()
