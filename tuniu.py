import pandas as pd
import requests
from bs4 import BeautifulSoup
import datetime
from decimal import Decimal, ROUND_DOWN
from urllib.parse import quote
import json
import re
import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED

class TuniuScraper():
    def __init__(self, pages):
        self.pages = pages
        self.comments = []
        self.star_levels = []
        self.check_max_page = True
        self.urls = []
        self.site_names = []
        self.sight_names = []

    def get_comment(self, url):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'
            }
            r = requests.get(url=url, headers=headers)

            decode_r = r.json()['data']

            soup = BeautifulSoup(decode_r, "lxml")
            main_content = soup.find_all('div', class_='item')

            # if self.check_max_page is True:
            #     max_page = soup.find('div', class_='page-info').contents[0][1]
            #     if max_page is None:
            #         self.urls = self.urls[0]
            #         print("only 1 page")
            #     else:
            #         self.urls = self.urls[0:max_page-1]
            #         print("max # pages: " + max_page)
            #     # set flag to false so won't check again
            #     self.check_max_page = False

            for para in main_content:
                comment = para.find('div', class_='content')
                self.comments.append(comment.text.replace('&quot', ''))

                top_list = para.find('div', class_='top')
                star_level_raw = top_list.find_all('span')[3]['class'][1]
                star_level = star_level_raw[-1:]
                self.star_levels.append(star_level)


        except Exception as err:
            print(err.with_traceback())

    def get_id(self, placename):

        driver = webdriver.Chrome()
        driver.get('https://www.tuniu.com')
        search = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, 'keyword-input')))
        search.clear()
        search.send_keys(placename)
        result = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//div[@class='resultbox an_mo']")))
        id = result.get_attribute('data-id')

        print(id)

        return id


    def scrappy(self, placename):
        id = self.get_id(placename)
        # create timestamp (len=13) for the current time, cast to decimal
        ts = Decimal(datetime.datetime.now().timestamp())
        # round down ts to 3 decimal places
        ts = 1000 * ts.quantize(Decimal('.001'), rounding=ROUND_DOWN)
        # get rid of the decimal point
        ts = ts.quantize(Decimal('0'), rounding=ROUND_DOWN)

        self.urls = [
            r"http://www.tuniu.com/newguide/api/widget/render/?widget=ask.AskAndCommentWidget&params%5BpoiId%5D=" + str(
                id) + "&params%5Bpage%5D=" + str(page) + r"&_=" + str(ts) for page in range(1, self.pages+1)]
        print(self.urls)

        executor = ThreadPoolExecutor(max_workers=20)
        future_tasks = [executor.submit(self.get_comment, url) for url in self.urls]
        wait(future_tasks, return_when=ALL_COMPLETED)

        for x in range(0, len(self.comments)):
            self.sight_names.append(placename)
            self.site_names.append('途牛')

        if not self.comments:
            print("nooooo record")
            self.quit_scraping(placename)

        comments_table = pd.DataFrame({'id': range(1, len(self.comments) + 1),
                                       'comments': self.comments,
                                       'star_levels' : self.star_levels})

        comments_table.to_csv("tuniu-" + placename + ".csv", index=False)

        print('done scrapping ' + placename + ' from tuniu')

    def quit_scraping(self, placename):
        print('No record of ' + placename + ' found on tuniu')
        return


if __name__ == "__main__":
    # s1 = TuniuScraper(10)
    # s1.scrappy("西湖")
    # s2 = TuniuScraper(10)
    # s2.scrappy("西湖")
    s3 = TuniuScraper(10)
    s3.scrappy("上海迪士尼乐园")
