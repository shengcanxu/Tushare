##
# 从雪球爬取美股当天的价格。 
# 爬取链接为： "https://stock.xueqiu.com/v5/stock/chart/kline.json?symbol=" + self.code + "&begin=1568252412387&period=day&type=before&count=-100000&indicator=kline,pe,pb,ps,pcf,market_capital,agt,ggt,balance"

from requests_html import HTMLSession
from helper.logger import FileLogger
import time
import json
import pandas as pd


DAY_TITLE_INDEX = ['timestamp', 'volume', 'open', 'high', 'low', 'close', 'chg',
                            'percent', 'turnoverrate', 'amount', 'volume_post', 'amount_post',
                            'pe', 'pb', 'ps', 'pcf', 'market_capital', 'balance',
                            'hold_volume_cn', 'hold_ratio_cn', 'net_volume_cn', 'hold_volume_hk',
                            'hold_ratio_hk', 'net_volume_hk']

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6"
}

COOKIES = {
    "device_id": "24700f9f1986800ab4fcc880530dd0ed",
    "xq_a_token": "a4b3e3e158cfe9745b677915691ecd794b4bf2f9",
    "xqat": "a4b3e3e158cfe9745b677915691ecd794b4bf2f9",
    "xq_r_token": "b80d3232bf315f8710d36ad2370bc777b24d5001",
    "xq_id_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOi0xLCJpc3MiOiJ1YyIsImV4cCI6MTYxNzc2MzQxOCwiY3RtIjoxNjE1MTg1MTM2ODU5LCJjaWQiOiJkOWQwbjRBWnVwIn0.AHJmRdGTwxGe7sgZelbUQfL_LkE49Dx4VCXv8qMvAaIVdO3qq1WRuBEUqMwhnozhlhr0Kq95ed1_lWRMyvew1uRRcZ3YoZh1FFejWadiNlFlWNdOTUqAeZSdj175ZqRSaahYfZiMhI73myOppsMuzdFSKTxGoSCQDeePZUcYwJfCP67bpUiE-KNZTjEMyc80BPvMfSexZtYa-rH4hNrjtO9xsh-nYTZP7xUbUi42qXP8bBW6SfyYp3TNSOd6wo_aM2KS3InfLJK_rhA90WuBWUPGmSoE2z6PSl_fIV044mhFL4x2cYQBq2xMSzJ7Sn3wLtN3Ge3glDY-k4B_YMIWVA",
    "u": "651615185139134",
    "Hm_lvt_1db88642e346389874251b5a1eded6e3": "1613914037,1614050366,1614653940,1615605267",
    "Hm_lpvt_1db88642e346389874251b5a1eded6e3": "1616468172"
}


# history stock data till 2021/03/24
def crawlHistory(code) -> bool:
    link = "https://stock.xueqiu.com/v5/stock/chart/kline.json?symbol=%s&begin=1616585707592&period=day&type=before&count=-100000&indicator=kline,pe,pb,ps,pcf,market_capital,agt,ggt,balance" % code
    session = HTMLSession()
    r = session.get(link, headers=HEADERS, cookies=COOKIES)

    jsonObj = json.loads(r.content)
    if jsonObj['error_code'] != 0 or not jsonObj["data"].__contains__("column") or not jsonObj["data"].__contains__("item"):
        FileLogger.error("get content error from: %s" % code)
        return False

    columns = jsonObj["data"]["column"]
    items = jsonObj["data"]["item"]

    if len(items) > 0:
        path = "C:/project/stockdata/UShistory/%s.csv" % code
        save2csv(columns, items, path)

    FileLogger.info("get %d lines from code: %s" % (len(items), code))
    return True


def save2csv(columns, items, filePath) -> bool:
    csvString = ""
    for index in range(len(columns)):
        if index != 0:
            csvString += ","
        csvString += columns[index]
    csvString += "\r"

    for item in items:
        for index in range(len(item)):
            if index != 0:
                csvString += ","
            csvString += str(item[index])
        csvString += "\r"

    return write2File(filePath, csvString)


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


def crawlUSStocks():
    # 查询语句：select ts_code from usstock.stocklist; 
    stockList = pd.read_csv("C:/project/Tushare/usstock/code.csv").to_numpy()

    for code in stockList:
        FileLogger.info("running on code: " + code[0])
        try:
            crawlHistory(code[0])
            time.sleep(1)
        except Exception as ex:
            FileLogger.error(ex)
            FileLogger.error("crawl error on code: %s" % code)
            time.sleep(5)


if __name__ == "__main__":
    crawlUSStocks()
