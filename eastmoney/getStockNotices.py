##
# 从巨潮资讯网爬取公告 
# 爬取链接为：http://www.cninfo.com.cn/new/hisAnnouncement/query
##

import sys
sys.path.append("C:/project/Tushare")
from requests_html import HTMLSession
from helper.logger import FileLogger
import time
import json
import pandas as pd


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6"
}

POSTDATA = {
    "pageNum": 1,
    "pageSize": 30,
    "column": "szse",
    "tabName": "fulltext",
    "plate": None,
    "stock": "",
    "searchkey": None,
    "secid": None,
    "category": None,
    "trade": None,
    "seDate": "2000-01-01~2021-08-19",
    "sortName": None,
    "sortType": None,
    "isHLtitle": True
}


# history notices data from 2011.1.1
def crawlStockNotices(code, orgId):
    records = []

    link = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
    session = HTMLSession()
    data = POSTDATA.copy()
    data["stock"] = "%s,%s" % (code, orgId)
    r = session.post(link, data=data, headers=HEADERS)
    if r.content:
        jsonContent = json.loads(r.content)
        totalpages = jsonContent["totalpages"]
        announcements = jsonContent["announcements"]
        records.extend(announcements)
        FileLogger.info("get records on code: %s of totalPages:%d" % (code, totalpages))

        for pageNum in range(2, totalpages+2):
            time.sleep(0.1)
            data["pageNum"] = pageNum
            r = session.post(link, data=data, headers=HEADERS)
            if r.content:
                jsonContent = json.loads(r.content)
                announcements = jsonContent["announcements"]
                records.extend(announcements)
                FileLogger.info("get records on pageNum: %d" % pageNum)
        
        FileLogger.info("get %d records on code: %s" % (len(records), code))

    if len(records) != 0:
        content = json.dumps(records)
        path = "C:/project/stockdata/StockNotices/%s.json" % code
        write2File(path, content)


def write2File(filePath, content, mode="w+", encoding='utf8') -> bool:
    try:
        fp = open(filePath, mode=mode, encoding=encoding)
        fp.write(content)
        fp.flush()
        fp.close()
        return True
    except Exception as ex:
        FileLogger.error("write to file error on path: %s" % filePath)
        FileLogger.error(ex)
        return False


# get json object from file
def getJsonFromFile(filePath):
    try:
        fp = open(filePath, mode='r', encoding='UTF8')
        text = fp.read()
        if text:
            jsonObjects = json.loads(text)
            return jsonObjects
        else:
            return []

    except Exception as ex:
        FileLogger.error(ex)
        return []


if __name__ == "__main__":
    stockList = getJsonFromFile("C:/project/stockdata/StockNotices/stock.json")
    stockList = stockList["stockList"]
    
    # stockList = [{"orgId":"9900002701","category":"A股","code":"002127","pinyin":"njds","zwjc":"南极电商"}]

    start = False
    for stock in stockList:
        FileLogger.info("running on stock: %s(%s)" % (stock["zwjc"], stock["code"]))
        if stock["code"] == '300398':
            start = True
        if start is False:
            continue
        
        try:
            crawlStockNotices(stock["code"], stock["orgId"])
            time.sleep(1)

        except Exception as ex:
            FileLogger.error(ex)
            FileLogger.error("crawl balance error on code: %s" % stock["code"])
            time.sleep(3)