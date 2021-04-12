##
# 从雪球爬取美股收入表。 
# 爬取链接为："https://stock.xueqiu.com/v5/stock/finance/us/income.json?symbol=" + self.code + "&type=all&is_detail=true&count=1000&timestamp=1568620992516"
##

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
    "xq_a_token": "a4b3e3e158cfe9745b677915691ecd794b4bf2f9",
    "xqat": "a4b3e3e158cfe9745b677915691ecd794b4bf2f9",
    "xq_r_token": "b80d3232bf315f8710d36ad2370bc777b24d5001",
    "xq_id_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOi0xLCJpc3MiOiJ1YyIsImV4cCI6MTYxNzc2MzQxOCwiY3RtIjoxNjE1MTg1MTM2ODU5LCJjaWQiOiJkOWQwbjRBWnVwIn0.AHJmRdGTwxGe7sgZelbUQfL_LkE49Dx4VCXv8qMvAaIVdO3qq1WRuBEUqMwhnozhlhr0Kq95ed1_lWRMyvew1uRRcZ3YoZh1FFejWadiNlFlWNdOTUqAeZSdj175ZqRSaahYfZiMhI73myOppsMuzdFSKTxGoSCQDeePZUcYwJfCP67bpUiE-KNZTjEMyc80BPvMfSexZtYa-rH4hNrjtO9xsh-nYTZP7xUbUi42qXP8bBW6SfyYp3TNSOd6wo_aM2KS3InfLJK_rhA90WuBWUPGmSoE2z6PSl_fIV044mhFL4x2cYQBq2xMSzJ7Sn3wLtN3Ge3glDY-k4B_YMIWVA",
    "u": "651615185139134",
    "Hm_lvt_1db88642e346389874251b5a1eded6e3": "1613914037,1614050366,1614653940,1615605267",
    "Hm_lpvt_1db88642e346389874251b5a1eded6e3": "1616468172"
}


# history income data till 2021/03/24
def crawlIncome(code):
    link = "https://stock.xueqiu.com/v5/stock/finance/us/income.json?symbol=%s&type=all&is_detail=true&count=1000&timestamp=1616585707592" % code
    session = HTMLSession()
    r = session.get(link, headers=HEADERS, cookies=COOKIES)

    content = json.dumps(json.loads(r.content))
    path = "C:/project/stockdata/USIncome/%s.json" % code
    write2File(path, content)
    FileLogger.info("get income of code: %s in size: %d" % (code, len(content)))
    

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
            crawlIncome(code)
            time.sleep(1)

        except Exception as ex:
            FileLogger.error(ex)
            FileLogger.error("crawl income error on code: %s" % code)
            time.sleep(3)
