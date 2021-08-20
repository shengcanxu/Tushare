##
# 从东方财富爬取利润表。 
# 爬取链接为：http://f10.eastmoney.com/NewFinanceAnalysis/lrbAjaxNew?companyType=4&reportDateType=0&reportType=1&dates=2019-12-31%2C2019-09-30%2C2019-06-30%2C2019-03-31%2C2018-12-31&code=SZ002127
##

import sys
sys.path.append("C:/project/Tushare")
from requests_html import HTMLSession
from helper.logger import FileLogger
import time
import json
import pandas as pd
from helper.util import write2File


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6"
}

DATES = [
    '2021-03-31%2C2020-12-31%2C2020-09-30%2C2020-06-30%2C2020-03-31',
    '2019-12-31%2C2019-09-30%2C2019-06-30%2C2019-03-31%2C2018-12-31',
    '2018-09-30%2C2018-06-30%2C2018-03-31%2C2017-12-31%2C2017-09-30',
    '2017-06-30%2C2017-03-31%2C2016-12-31%2C2016-09-30%2C2016-06-30',
    '2016-03-31%2C2015-12-31%2C2015-09-30%2C2015-06-30%2C2015-03-31',
    '2014-12-31%2C2014-09-30%2C2014-06-30%2C2014-03-31%2C2013-12-31',
    '2013-09-30%2C2013-06-30%2C2013-03-31%2C2012-12-31%2C2012-09-30',
    '2012-06-30%2C2012-03-31%2C2011-12-31%2C2011-09-30%2C2011-06-30',
    '2011-03-31%2C2010-12-31%2C2010-09-30%2C2010-06-30%2C2010-03-31',
    '2009-12-31%2C2009-09-30%2C2009-06-30%2C2009-03-31%2C2008-12-31',
    '2008-09-30%2C2008-06-30%2C2008-03-31%2C2007-12-31%2C2007-09-30',
    '2007-06-30%2C2007-03-31%2C2006-12-31%2C2006-09-30%2C2006-06-30',
    '2006-03-31%2C2005-12-31%2C2005-09-30%2C2005-06-30%2C2005-03-31',
    '2004-12-31%2C2004-09-30%2C2004-06-30%2C2004-03-31%2C2003-12-31',
    '2003-09-30%2C2003-06-30%2C2003-03-31%2C2002-12-31%2C2002-09-30',
    '2002-06-30%2C2002-03-31%2C2001-12-31%2C2001-06-30%2C2000-12-31',
    '2000-06-30%2C1999-12-31%2C1999-06-30%2C1998-12-31%2C1998-06-30',
    '1997-12-31%2C1997-06-30%2C1996-12-31%2C1996-06-30%2C1995-12-31',
]


# history income data till 2021/03/24
def crawlIncome(code, companyType):
    records = []
    for date in DATES:
        link = "http://f10.eastmoney.com/NewFinanceAnalysis/lrbAjaxNew?companyType=%d&reportDateType=0&reportType=1&dates=%s&code=%s" % (companyType, date, code)
        session = HTMLSession()
        r = session.get(link, headers=HEADERS)
        jsonContent = json.loads(r.content)
        if "data" not in jsonContent:
            FileLogger.info("no more data on %s at dates: %s" % (code, date))
            break
        for obj in jsonContent["data"]:
            records.append(obj)
      
        FileLogger.info("get income of code: %s in size: %d" % (code, len(jsonContent["data"])))
        # time.sleep(0.5)

    if len(records) != 0:
        content = json.dumps(records)
        # path = "C:/project/stockdata/EastMoneyIncome/%s.json" % code
        path = "C:/project/stockdata/backup/%s.json" % code
        write2File(path, content)


if __name__ == "__main__":
    # 查询语句：SELECT ts_code FROM stock.stockdata;
    stockdf = pd.read_csv("C:/project/Tushare/eastmoney/code123.csv")
    stockList = stockdf['ts_code'].to_numpy()

    for code in stockList:
        FileLogger.info("running on code: %s" % code)
        try:
            crawlIncome(code, 4)
            time.sleep(1)

        except Exception as ex:
            FileLogger.error(ex)
            FileLogger.error("crawl income error on code: %s" % code)
            time.sleep(3)

    # stockdf = pd.read_csv("C:/project/Tushare/eastmoney/codewithcompanytype.csv")
    # stockList = stockdf[stockdf.companytype != 4].to_numpy()

    # for list in stockList:
    #     code = list[0]
    #     companyType = list[1]
    #     FileLogger.info("running on code: %s" % code)
    #     try:
    #         crawlIncome(code, companyType)
    #         time.sleep(1)

    #     except Exception as ex:
    #         FileLogger.error(ex)
    #         FileLogger.error("crawl balance error on code: %s" % code)
    #         time.sleep(3)