from requests_html import HTMLSession
from helper.logger import FileLogger
import time
import json
import pandas as pd


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6"
}

COOKIES = {
    "device_id": "24700f9f1986800ab4fcc880530dd0ed",
    "Hm_lvt_1db88642e346389874251b5a1eded6e3": "1615605267", 
    "s": "du115uv2rh", 
    "xq_a_token": "cc6a2aedef8a96868eb7257aef4a2ba6e222d2c6", 
    "xqat": "cc6a2aedef8a96868eb7257aef4a2ba6e222d2c6", 
    "xq_r_token": "3e168659e8b7d1863aff7a493cfc3398f438abe3", 
    "xq_id_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOi0xLCJpc3MiOiJ1YyIsImV4cCI6MTYxOTkyMzQ2NiwiY3RtIjoxNjE3NjkzOTUwMTk5LCJjaWQiOiJkOWQwbjRBWnVwIn0.HXeHCs4v3axsiPWbMJ68aY509Hk4yWQRM4DUrTpUCTA4OS-beIOQqV1JaQsUAea2HrgmZI4ljBAckOJFmd0KgXVIS_pifKHFTCUYJvQ5tyPevIdKmOm3zENjR9r7-T0vL0pYhl_sDs5EJ-wtEiat6tvsDkSXiJ0d1otQxlhdghCkZoEWhayP5ZAH7FBRkipMPbu3Z0CTLETWVWWiZBUPwS7PkIC3vUV3aGiJeIeC48hpuOR1m3WkN7ftFg-XcCRm5-z1Omc6GKUAkIl4fcpE11BooyXiWGzxIo6Ku0jqkvkKFI1b7YtDJpYKAZtrgnAWdIl97dO-XgyWu5rA2ZQ3aw", 
    "u": "611617693997058", 
    "Hm_lpvt_1db88642e346389874251b5a1eded6e3": "1617694417"
}


# history cash data till 2021/03/24
def crawlCashflow(code):
    link = "https://stock.xueqiu.com/v5/stock/finance/us/cash_flow.json?symbol=%s&type=all&is_detail=true&count=1000&timestamp=1616585707592" % code
    session = HTMLSession()
    r = session.get(link, headers=HEADERS, cookies=COOKIES)

    content = json.dumps(json.loads(r.content))
    path = "C:/project/stockdata/USCashflow/%s.json" % code
    write2File(path, content)
    FileLogger.info("get cashflow of code: %s in size: %d" % (code, len(content)))


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


if __name__ == "__main__":
    # 查询语句：select ts_code from usstock.stocklist; 
    stockdf = pd.read_csv("C:/project/Tushare/usstock/code.csv")
    errordf = pd.read_csv("C:/project/Tushare/usstock/get_error_ts_code.csv")
    errorList = errordf['ts_code'].to_numpy()
    stockList = stockdf[~stockdf['ts_code'].isin(errorList)]
    stockList = stockList['ts_code'].to_numpy()

    for code in stockList:
        FileLogger.info("running on code: %s" % code)
        try:
            crawlCashflow(code)
            time.sleep(1)

        except Exception as ex:
            FileLogger.error(ex)
            FileLogger.error("crawl cashflow error on code: %s" % code)
            time.sleep(3)