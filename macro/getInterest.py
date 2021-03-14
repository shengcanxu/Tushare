# 国内宏观利率

import tushare as ts
from sqlalchemy import create_engine
import time
import pandas as pd
import datetime

ts.set_token('803f1548c1f25bf44c56644e4527a6d8cd3dbd8517e7c59e3aa1f6d0')
pro = ts.pro_api()
macroEngine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/macro?charset=utf8")


if __name__ == "__main__":

    # # SHIBOR利率数据
    # begin = datetime.date(1990, 12, 10)
    # end = datetime.date(2021, 2, 25)

    # date = begin
    # delta = datetime.timedelta(days=500)
    # while date <= end:
    #     start = date
    #     date += delta

    #     shibor = pro.shibor(start_date=start.strftime("%Y%m%d"), end_date=date.strftime("%Y%m%d"))
    #     shibor = shibor.set_index(["date"])
    #     shibor.to_sql(name="shibor", con=macroEngine, if_exists='append')
    #     print("get macro interest record: %d" % len(shibor))
    #     time.sleep(1)


    # # SHIBOR报价数据
    # begin = datetime.date(1990, 12, 10)
    # end = datetime.date(2021, 2, 25)

    # date = begin
    # delta = datetime.timedelta(days=100)
    # while date <= end:
    #     start = date
    #     date += delta

    #     quote = pro.shibor_quote(start_date=start.strftime("%Y%m%d"), end_date=date.strftime("%Y%m%d"))
    #     quote = quote.set_index(["date", "bank"])
    #     quote.to_sql(name="shiborquote", con=macroEngine, if_exists='append')
    #     print("get macro interest quote record: %d" % len(quote))
    #     time.sleep(1)
    

    # # SHIBOR LPR数据
    # begin = datetime.date(1990, 12, 10)
    # end = datetime.date(2021, 2, 25)

    # date = begin
    # delta = datetime.timedelta(days=500)
    # while date <= end:
    #     start = date
    #     date += delta

    #     lpr = pro.shibor_lpr(start_date=start.strftime("%Y%m%d"), end_date=date.strftime("%Y%m%d"))
    #     lpr = lpr.set_index(["date"])
    #     lpr.to_sql(name="shiborlpr", con=macroEngine, if_exists='append')
    #     print("get macro interest LPR record: %d" % len(lpr))
    #     time.sleep(1)


    # # LIBOR拆借利率
    # begin = datetime.date(1990, 12, 10)
    # end = datetime.date(2021, 2, 25)

    # # USD美元 EUR欧元 JPY日元 GBP英镑 CHF瑞郎
    # for currency in ["USD", "EUR", "JPY", "GBP", "CHF"]: 
    #     date = begin
    #     delta = datetime.timedelta(days=500)
    #     while date <= end:
    #         start = date
    #         date += delta

    #         libor = pro.libor(curr_type=currency, start_date=start.strftime("%Y%m%d"), end_date=date.strftime("%Y%m%d"))
    #         libor = libor.set_index(["date", "curr_type"])
    #         libor.to_sql(name="libor", con=macroEngine, if_exists='append')
    #         print("get macro libor interest record: %d on currency: %s " % (len(libor), currency))
    #         time.sleep(1)


    # # HIBOR利率
    # begin = datetime.date(1990, 12, 10)
    # end = datetime.date(2021, 2, 25)

    # date = begin
    # delta = datetime.timedelta(days=500)
    # while date <= end:
    #     start = date
    #     date += delta

    #     hibor = pro.hibor(start_date=start.strftime("%Y%m%d"), end_date=date.strftime("%Y%m%d"))
    #     hibor = hibor.set_index(["date"])
    #     hibor.to_sql(name="hibor", con=macroEngine, if_exists='append')
    #     print("get hibor interest record: %d" % len(hibor))
    #     time.sleep(1)


    # # 温州民间借贷利率
    # begin = datetime.date(1990, 12, 10)
    # end = datetime.date(2021, 2, 25)

    # date = begin
    # delta = datetime.timedelta(days=500)
    # while date <= end:
    #     start = date
    #     date += delta

    #     wzindex = pro.wz_index(start_date=start.strftime("%Y%m%d"), end_date=date.strftime("%Y%m%d"))
    #     wzindex = wzindex.set_index(["date"])
    #     wzindex.to_sql(name="wzindex", con=macroEngine, if_exists='append')
    #     print("get wzindex interest record: %d" % len(wzindex))
    #     time.sleep(1)

    # 广州民间借贷利率
    begin = datetime.date(1990, 12, 10)
    end = datetime.date(2021, 2, 25)

    date = begin
    delta = datetime.timedelta(days=500)
    while date <= end:
        start = date
        date += delta

        gzindex = pro.gz_index(start_date=start.strftime("%Y%m%d"), end_date=date.strftime("%Y%m%d"))
        gzindex = gzindex.set_index(["date"])
        gzindex.to_sql(name="gzindex", con=macroEngine, if_exists='append')
        print("get gzindex interest record: %d" % len(gzindex))
        time.sleep(1)
