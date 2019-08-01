import pandas as pd
import requests
from bs4 import BeautifulSoup
import datetime
from decimal import Decimal, ROUND_DOWN
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import json
from urllib.parse import quote
import re
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


class MafengwoScraper():
    def __init__(self, pages):
        self.comments = []
        self.star_levels = []
        self.pages = pages
        self.sight_names = []
        self.site_names = []

    def get_comment(self, url, id):
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
                       'Referer': "http://www.mafengwo.cn/poi/" + str(id) + ".html"
                       }
            try:
                # make a request
                r = requests.get(url=url, headers=headers)
            except Exception:
                pass

            decoded_r = r.content.decode('utf-8')
            dict_r = json.loads(decoded_r)
            html = dict_r['data']['html']
            soup = BeautifulSoup(html, 'lxml')
            # clean_soup = soup.prettify()
            main_content = soup.find_all('li', class_='rev-item comment-item clearfix')

            for para in main_content:
                comment = para.find_all('p', class_='rev-txt')
                c = comment[0].contents[0]
                self.comments.append(c)

                star_level_raw = para.find_all('span', limit=3)[2]['class'][1]
                star_level = star_level_raw[-1:]
                self.star_levels.append(star_level)

        except Exception as err:
            print(err.with_traceback())


    def get_id(self, placename):
        driver = webdriver.Chrome()
        driver.get('http://www.mafengwo.cn/mdd/')
        # time.sleep(5)
        search = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, '_j_head_search_input')))
        search.send_keys(placename)
        result = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//li[@data-type='pois']")))
        url = result.get_attribute('data-url')
        if url is None:
            result = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//li[@class='mss-item _j_listitem active']")))
            url = result.get_attribute('data-url')
            if url is None:
                # close browser and stop scrapping if no such url is found
                driver.close()
                self.quit_scraping(placename)

        match = re.findall('(\/[0-9]*\.)', url)
        if not match:
            match = re.findall('(\=[0-9]*\&)', url)
            if not match:
                driver.close()
                self.quit_scraping(placename)

        id = match[0][1:][:-1]

        driver.close()

        return id

    def scrappy(self, placename):
        # create timestamp (len=13) for the current time, cast to decimal
        ts = Decimal(datetime.datetime.now().timestamp())
        # round down ts to 3 decimal places
        ts = 1000 * ts.quantize(Decimal('.001'), rounding=ROUND_DOWN)
        # get rid of the decimal point
        ts = ts.quantize(Decimal('0'), rounding=ROUND_DOWN)

        id = self.get_id(placename)

        urls = [r"http://pagelet.mafengwo.cn/poi/pagelet/poiCommentListApi?"
                + r"&params=%7B%22poi_id%22%3A%22" + str(id) + r"%22%2C%22page%22%3A" + str(page)
                + r"%2C%22just_comment%22%3A1%7D&_ts=" + str(ts) + "&_=" + str(ts) for page in range(1, self.pages + 1)]

        # multithread to speed things up
        executor = ThreadPoolExecutor(max_workers=20)
        future_tasks = [executor.submit(self.get_comment, url, id) for url in urls]
        wait(future_tasks, return_when=ALL_COMPLETED)

        for x in range(0, len(self.comments)):
            self.sight_names.append(placename)
            self.site_names.append('马蜂窝')

        # no comments found quit scraping for this place
        if not self.comments:
            print("no comments found")
            self.quit_scraping(placename)

        comments_table = pd.DataFrame({'id': range(1, len(self.comments) + 1),
                                       'comments': self.comments,
                                       'star_levels': self.star_levels})

        comments_table.to_csv('mfw-' + placename + r".csv", index=False)

        print('done scrapping' + placename + ' from mafengwo')

    def quit_scraping(self, placename):
        print('No record of ' + placename + ' found on mafengwo')
        return


if __name__ == "__main__":
    # s1 = MafengwoScraper(10)
    # s1.scrappy('1093', 'mfw-xihu')
    # s2 = MafengwoScraper(10)
    # s2.scrappy("故宫")
    # s3 = MafengwoScraper(10)
    # s3.scrappy("网师园")
    s5 = MafengwoScraper(10)
    s5.scrappy("中山陵")
    # s3 = MafengwoScraper(10)
    # s3.scrappy("泰山")
