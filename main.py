from bs4 import BeautifulSoup
from urllib.parse import urlunsplit, urlencode
import os

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

from selenium.webdriver.common.by import By

service = webdriver.chrome.service.Service(os.path.abspath("chromedriver"))
service.start()

chrome_options = Options()
chrome_options.add_argument("--headless")

# path to the binary of Chrome Canary that we installed earlier
chrome_options.binary_location = '/Applications/Google Chrome   Canary.app/Contents/MacOS/Google Chrome Canary'

driver = webdriver.Remote(service.service_url, desired_capabilities=chrome_options.to_capabilities())


def get_soup_from(url, keyword, offset=0, lang='it'):
    url_ = url + '?' + urlencode({
        'q': keyword,
        'offset': offset,
        'lang': lang
    })

    print(url_)
    driver.get(url_)

    return BeautifulSoup(driver.page_source, 'html.parser')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    soup = get_soup_from('https://www.change.org/search', 'covid', )
    print(soup.title.string)

    for result in soup.find_all('div.search-result'):
        print(result.find('h3'))

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
