from requests_html import HTMLSession
from helper import *
from crawler.stockCrawler import StockCrawler
from logger import FileLogger
import time


class Balance(StockCrawler):

    def __init__(self, code) -> None:
        self.code = code

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6"
        }

        self.cookies = {
            # "_ga":"GA1.2.1790180301.1555399578",
            # "device_id":"0f7a0138c07ea676894dd81e4975d93d",
            # "s":"c411ptvwim",
            # "xq_a_token.sig":"x1KC7p7wWjKTj7oCXq2J6FnZfjw",
            # "xq_r_token.sig":"KZo257TCfq7RFChDvdtVeD_9sog",
            # "Hm_lvt_1db88642e346389874251b5a1eded6e3":"1567583917,1567997612",
            # "xq_token_expire":"Fri%20Oct%2004%202019%2017%3A32%3A15%20GMT%2B0800%20(China%20Standard%20Time)",
            # "bid":"deedac848c890be06868ef62034afc25_k0c7mnl5",
            "xq_a_token": "75661393f1556aa7f900df4dc91059df49b83145",
            # "xq_r_token":"29fe5e93ec0b24974bdd382ffb61d026d8350d7d",
            # "u":"551568166262249",
            # "Hm_lpvt_1db88642e346389874251b5a1eded6e3":"1568172975"
        }
        self.content = ""

    #history income data till 2019/09/10
    def crawlHistory(self) -> bool:
        link = "https://stock.xueqiu.com/v5/stock/finance/us/balance.json?symbol=" + self.code +"&type=all&is_detail=true&count=1000&timestamp=1568628254995"
        session = HTMLSession()
        r = session.get(
            link,
            headers=self.headers,
            cookies=self.cookies
            )

        jsonObject = json.loads(r.content)
        if len(jsonObject["data"]["list"]) == 0:
            self.content = ""
            FileLogger.error("get content error from: " + self.code + ", no content!")
            return False
        else:
            self.content = json.dumps(jsonObject)
            return True

    def crawlNew(self) -> bool:
        pass

    def save2file(self,filePath) -> bool:
        return write2File(filePath,self.content)


if __name__ == "__main__":
    # get stock balance for all US stocks
    start = 0
    stockCodes = getStockCode("USStockCode.txt")
    for index in range(start,len(stockCodes)):
        code = stockCodes[index]
        FileLogger.info("running on code: " + code)

        try:
            balance = Balance(code)
            if balance.crawlHistory():
                csv = balance.save2file("D:/project/crawler/data/USbalance/" + code + ".json")
                FileLogger.info("get balance content from code: " + code)

            time.sleep(1)
        except:
            FileLogger.error("crawl balance error on code: " + code)
            time.sleep(60)
