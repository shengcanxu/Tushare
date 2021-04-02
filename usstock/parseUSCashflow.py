import pandas as pd
from logger import FileLogger
from sqlalchemy import create_engine
import time
import json

usEngine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/usstock?charset=utf8")

BASE_COLUMNS = ('ts_code', 'annual_settle_date', 'currency', 'currency_name',
                'last_report_name', 'org_type', 'quote_name', 'sas',
                'statuses', 'tip')
COLUMNS = (
    "ts_code",
    "report_date",
    "report_name",
    "ctime",
    "report_type_code",
    "report_annual",
    "sd",
    "ed",
    "net_cash_provided_by_oa",    "net_cash_provided_by_oa_rate",
    "net_cash_used_in_ia",    "net_cash_used_in_ia_rate",
    "net_cash_used_in_fa",    "net_cash_used_in_fa_rate",
    "payment_for_property_and_equip",    "payment_for_property_and_equip_rate",
    "effect_of_exchange_chg_on_cce",    "effect_of_exchange_chg_on_cce_rate",
    "cce_at_boy",    "cce_at_boy_rate",
    "cce_at_eoy",    "cce_at_eoy_rate",
    "increase_in_cce",    "increase_in_cce_rate",
    "depreciation_and_amortization",    "depreciation_and_amortization_rate",
    "operating_asset_and_liab_chg",    "operating_asset_and_liab_chg_rate",
    "purs_of_invest",    "purs_of_invest_rate",
    "common_stock_issue",    "common_stock_issue_rate",
    "repur_of_common_stock",    "repur_of_common_stock_rate",
    "dividend_paid",    "dividend_paid_rate"
)

cashflowBaseDF = pd.DataFrame(columns=BASE_COLUMNS)


def parseUSCashflow(code):
    path = "C:/project/stockdata/USCashflow/%s.json" % code
    text = readFile(path)
    jsonObj = json.loads(text)
    jsonObjList = jsonObj['data']['list']

    cashflowDF = pd.DataFrame(columns=COLUMNS)
    jsonData = {'ts_code': code}
    for data in jsonObjList:
        for key, value in data.items():
            if isinstance(value, list):
                jsonData[key] = value[0]
                jsonData[key+'_rate'] = value[1]
            else:
                jsonData[key] = value
        cashflowDF = cashflowDF.append([jsonData], ignore_index=True)
    
    cashflowDF = cashflowDF.drop_duplicates(['ts_code', 'report_date'])
    cashflowDF = cashflowDF.set_index(['ts_code', 'report_date'])
    cashflowDF.to_sql(name='cashflow', con=usEngine, if_exists='append')


def parseCashflowBase(code):
    path = "C:/project/stockdata/USCashflow/%s.json" % code
    text = readFile(path)

    if text:
        jsonObj = json.loads(text)
        jsonData = jsonObj['data']
        del jsonData['list']
        jsonData['ts_code'] = code

        global cashflowBaseDF
        cashflowBaseDF = cashflowBaseDF.append([jsonData], ignore_index=True)


def parseCashflowBaseList(codeList):
    for code in codeList:
        parseCashflowBase(code)

    global cashflowBaseDF
    cashflowBaseDF = cashflowBaseDF.drop_duplicates(['ts_code'])
    cashflowBaseDF = cashflowBaseDF.set_index(['ts_code'])

    cashflowBaseDF.to_sql(name='cashflowbase',
                          con=usEngine,
                          if_exists='append')


def readFile(filePath):
    try:
        fp = open(filePath, 'r')
        content = fp.read()
        return content
    except Exception as ex:
        FileLogger.error("read file error on path: %s" % filePath)
        FileLogger.error(ex)
        return False


if __name__ == "__main__":

    stockdf = pd.read_csv("C:/project/stockdata/USCashflow/code.csv")
    stockList = stockdf['code'].to_numpy()

    # parseCashflowBaseList(stockList)

    for code in stockList:
        FileLogger.info("running on code: %s" % code)
        try:
            parseUSCashflow(code)

        except Exception as ex:
            FileLogger.error(ex)
            FileLogger.error("write data to Database error on code: %s" % code)
            time.sleep(1)