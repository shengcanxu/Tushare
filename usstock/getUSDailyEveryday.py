##
# 从东方财富爬取美股当天的价格。 
# 爬取链接为： "http://14.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112403747648089642832_1570775302405&pn=1&pz=100000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:105,m:106,m:107&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152&_=1570775302461"
##

from requests_html import HTMLSession
from helper.logger import FileLogger
import time


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    # "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6"
}
COOKIES = {}


# from http://quote.eastmoney.com/center/gridlist.html#us_stocks
def crawlLatestUsStocks():
    link = "http://14.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112403747648089642832_1570775302405&pn=1&pz=100000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:105,m:106,m:107&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152&_=1570775302461"
    session = HTMLSession()
    r = session.get(
        link,
        headers=HEADERS,
        cookies=COOKIES
    )

    content = str(r.content, encoding='utf8').replace("jQuery112403747648089642832_1570775302405(", "").replace(");", "")
    return content


def write2File(filePath, content, mode="w+") -> bool:
    try:
        fp = open(filePath, mode)
        fp.write(content)
        fp.flush()
        fp.close()
        return True
    except Exception as ex:
        FileLogger.error("write to file error on path: %s" % filePath)
        FileLogger.error(ex)
        return False


def gettodayStock():
    curDate = time.strftime("%Y%m%d", time.localtime())
    tryagain = True
    while tryagain:
        try:
            content = crawlLatestUsStocks()
            if content:
                path = "C:/project/stockdata/USDay/%s.txt" % curDate
                write2File(path, content, mode="w")
                FileLogger.info("crawl stock list successfully on date:" + curDate)
                tryagain = False
            else:
                time.sleep(60)
        except Exception as ex:
            FileLogger.error(ex)
            FileLogger.error("crawl stock list error, retry in 60 seconds")
            time.sleep(60)


if __name__ == "__main__":
    gettodayStock()


