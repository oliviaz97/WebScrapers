import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote
import json
import datetime
from decimal import Decimal, ROUND_DOWN
from pypinyin import pinyin, Style
import re
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED


class CtripScraper():

    def __init__(self, pages):
        self.page_indices = list(range(1, pages))
        self.comments = []
        self.star_levels = []
        self.sight_names = []
        self.site_names = []

    def get_comment(self, urls, page):

        try:
            headers = {
                        # ':path' : '/ sight / chengdu28 / 4227 - dianping - p1.html',
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
                        'cookie': '_abtest_userid=313d78e4-58e9-4344-9283-fbf03857acb3; ibu_wws_c=1566617224131%7Czh-cn; MKT_Pagesource=PC; _RSG=JdK33h5IpKCoI3CDI4r3.A; _RDG=28aa73fd8f37c823f63fff34f394d7fa95; _RGUID=c533485a-c30d-49d3-af83-d4448d3034b2; Session=smartlinkcode=U135371&smartlinklanguage=zh&SmartLinkKeyWord=&SmartLinkQuary=&SmartLinkHost=; Union=AllianceID=4899&SID=135371&OUID=&Expires=1564709636256; TicketSiteID=SiteID=1004; ASP.NET_SessionSvc=MTAuOC4xODkuNTh8OTA5MHxqaW5xaWFvfGRlZmF1bHR8MTU1NzgxMzQxNDE3Ng; _RF1=35.197.1.135; _bfa=1.1564025219452.1t9llu.1.1564111258853.1564114246290.7.40.10650014170; _bfs=1.5; _jzqco=%7C%7C%7C%7C%7C1.33664493.1564025236757.1564114438999.1564114450305.1564114438999.1564114450305.0.0.0.33.33; __zpspc=9.8.1564114288.1564114450.4%233%7Cwww.google.com%7C%7C%7C%7C%23; appFloatCnt=23'
            }
            r = requests.get(url=urls[page], headers=headers)

            soup = BeautifulSoup(r.text, 'lxml')
            main_content = soup.find_all('div', class_='comment_single')

            for para in main_content:
                comment = para.find('span', class_='heightbox')
                self.comments.append(comment.text.replace('&quot', ''))
                stars = para.find('span', class_='starlist')
                stars = stars.find('span')['style']
                stars = re.sub('[%,;]', '', stars) # remove last two chars
                s = int(int(stars[6:])/20)

                self.star_levels.append(s)

            self.page_indices.remove(page)

        except Exception as err:
            print(err)

    def get_id(self, placename):
        # create timestamp (len=13) for the current time, cast to decimal
        ts = Decimal(datetime.datetime.now().timestamp())
        # round down ts to 3 decimal places
        ts = 1000 * ts.quantize(Decimal('.001'), rounding=ROUND_DOWN)
        # get rid of the decimal point
        ts = ts.quantize(Decimal('0'), rounding=ROUND_DOWN)

        decoded_str = quote(placename)

        url = "http://m.ctrip.com/restapi/h5api/globalsearch/search?action=online&source=globalonline&keyword=" + decoded_str + "&t=" + str(ts)

        headers = {
                    'Origin' : 'https://www.ctrip.com',
                    'Referer' : 'https://www.ctrip.com/',
                    'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'
                  }
        r = requests.get(url, headers)
        clean_r = r.content.decode('utf-8')
        dict_r = json.loads(clean_r)

        for x in range(0, len(dict_r['data'])):
            # try finding - to separate the id
            # only extract id if type is sight, otherwise move onto next in the list
            if(dict_r['data'][x]['type'] == 'sight'):
                try:
                    # id_dict = re.split('\-+', url)  # split the url into two parts, second part is the id we want
                    # id = id_dict[1]
                    id = dict_r['data'][x]['id']
                    district_name = dict_r['data'][x]['districtName']
                    city_id = dict_r['data'][x]['cityId']
                    break # found id, exit for loop

                # didn't find id, try the next match
                except Exception:
                    # print("no luck, next match")
                    continue
            else:
                # print("no match found")
                return

        # print(id)
        if(id == None or district_name == None or city_id == None):
            return

        return id, district_name, city_id


    def get_district_id(self, district_name):
        list = pinyin(district_name, style=Style.NORMAL)
        result = []
        for l in list:
            result.append(l[0])

        return ''.join(result)

    def scrappy(self, placename):
        # comments = []
        # star_levels = []
        urls = {}
        page_indices = list(range(1, 11))

        # deals with when no match of the demanded place was found
        if self.get_id(placename) is None:
            print("No match for " + placename + " was found")
            return

        sight_id, district_name, city_id = self.get_id(placename)
        district_id = self.get_district_id(district_name)

        # generate list of urls
        for p in self.page_indices:
            # creates url for a new page
            new_url = "https://you.ctrip.com/sight/" + district_id + str(city_id) + "/" + str(sight_id) + "-dianping-p%d.html" %p
            # adds a new pagenumber-url pair to the urls dict
            urls.update({p : new_url})

        # print(urls)
        executor = ThreadPoolExecutor(max_workers=20)
        future_tasks = []
        # for page in self.page_indices:
        #     future_tasks.append(executor.submit(self.get_comment, urls, page))
        # future_tasks = [executor.submit(self.get_comment, urls, ) for url in urls]
        while len(self.page_indices) != 0:
            # print(self.page_indices)
           # list of future tasks to be executed
            # future_tasks = []
            # try scrapping all indices left in the list
            for page in self.page_indices:
                self.get_comment(urls, page)

        # wait(future_tasks, return_when=ALL_COMPLETED)

        for x in range(0, len(self.comments)):
            self.sight_names.append(placename)
            self.site_names.append('携程')

        comments_table = pd.DataFrame({'id': range(1, len(self.comments) + 1),
                                       'comments': self.comments,
                                       'star_levels' : self.star_levels})

        comments_table.to_csv("ctrip-" + placename + r".csv", index=False)

        print("Done scraping " + placename + " from ctrip")


if __name__ == "__main__":
    # s1 = CtripScraper(10)
    # s1.scrappy("石路")
    s2 = CtripScraper(10)
    s2.scrappy("故宫")
    s3 = CtripScraper(10)
    s3.scrappy("拙政园")
