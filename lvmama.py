import requests
import gzip
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import threading
import datetime
from decimal import Decimal, ROUND_DOWN
import re
import json


class LvmamaScraper():
    # ctor - pages - # pages to be scrapped for dests on this website
    def __init__(self, pages):
        self.page_indices = list(range(1, pages + 1))
        self.comments = []
        self.star_levels = []
        self.sight_names = []
        self.site_names = []

    def get_comments(self, page, id):

        try:
            # create new urls to be scrapped from
            params = {
                      'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'
                       }

            r = requests.post('http://ticket.lvmama.com/vst_front/comment/newPaginationOfComments', data={
                'type' : 'all',
                'currentPage' : str(page),
                'totalCount' : '1741',
                'placeId' : str(id),
                'productId' : '',
                'placeIdType' : 'PLACE',
                'isPicture' : '',
                'isBest' : '',
                'isPOI' : 'Y',
                'isELong' : 'N',
                }, params=params)

            soup = BeautifulSoup(r.text, 'html.parser')
            texts = soup.find_all('div', class_='comment-li')

            for t in texts:
                temp = t.find('div', class_='ufeed-content')
                # remove all tabs/newlines/whitespaces
                x = "".join(temp.text.split())
                self.comments.append(x)
                y = t.find('span', class_='ufeed-level')
                y = y.find('i')["data-level"]
                self.star_levels.append(y)

            self.page_indices.remove(page)

        except Exception as err:
            print(err)

    # function that scraps the place id from entering keyword in search box
    def get_id(self, place_name):
        # create timestamp (len=13) for the current time, cast to decimal
        ts = Decimal(datetime.datetime.now().timestamp())
        # round down ts to 3 decimal places
        ts = 1000 * ts.quantize(Decimal('.001'), rounding=ROUND_DOWN)
        # get rid of the decimal point
        ts = ts.quantize(Decimal('0'), rounding=ROUND_DOWN)

        params = {'callback' : 'recive',
                  'keyword' : place_name,
                  'type' : "TICKET",
                  '_' : str(ts)
                 }

        request_url = "http://s.lvmama.com/autocomplete/autoCompleteNew.do"
        r = requests.get(request_url, params)
        # clean up r to become json format
        clean_r = r.content[7:len(r.content)-2].decode('utf-8')
        dict_r = json.loads(clean_r)


        for x in range(0, len(dict_r['matchList'])):
            try:
                id = dict_r['matchList'][x]['urlId']
                break # found id, exit for loop

            # didn't find id, try the next match
            except Exception:
                # print("no luck, next match")
                continue

        return id


    def scrappy(self, placename):

        id = self.get_id(placename)
        # fetch the reviews for all desired pages
        # for page in page_indices:
        #     get_reviews(page, id, page_indices, reviews)

        executor = ThreadPoolExecutor(max_workers=1)

        # keep scraping unscrapped pages until index list is empty (all page scrapped)
        while len(self.page_indices) != 0:
            # print(page_indices)
           # list of future tasks to be executed
            future_tasks = []
            # try scrapping all indices left in the list
            for page in self.page_indices:
                self.get_comments(page, id)
                # future_tasks.append(executor.submit(get_reviews, page, id, page_indices, reviews, star_levels, lock))
                # wait for this thread to be completed, move on to the next one
            # wait(future_tasks, 5, return_when=ALL_COMPLETED)

        for x in range(0, len(self.comments)):
            self.sight_names.append(placename)
            self.site_names.append('驴妈妈')

        # create dataframe, save to .csv
        comments_table = pd.DataFrame({'id': range(1, len(self.comments) + 1),
                                       'comments': self.comments,
                                       'star levels': self.star_levels})

        comments_table.to_csv("lmm-" + placename + r".csv", index=False)
        print("done scraping " + placename + " from lvmama")


if __name__ == "__main__":

    s1 = LvmamaScraper(10) # lmm_scraper instance to scrap 10 pages of reviews
    s1.scrappy("东方明珠")
    s2 = LvmamaScraper(10)
    s2.scrappy("西湖")
