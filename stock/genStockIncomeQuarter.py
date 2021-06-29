# 计算利润表 重要指标的按季度单算数据
import sys
sys.path.append("C:/project/Tushare")
from sqlalchemy import create_engine
import datetime
import helper.getDataFromDB as DBLib
from helper.logger import FileLogger
import pandas as pd
import math


engine = create_engine("mysql+pymysql://root:4401821211@localhost:3306/stock?charset=utf8")


def generateQuarterRate(code):
    income = DBLib.getIncomeFromDB(code)
    sortedIncome = income.sort_values(by='end_date', ascending=True)
    
    # dataList = sortedIncome[['end_date', 'total_revenue']]
    # generateQuarterData(code, dataList, 'total_revenue')

    # dataList = sortedIncome[['end_date', 'revenue']]
    # generateQuarterData(code, dataList, 'revenue')

    # dataList = sortedIncome[['end_date', 'total_cogs']]
    # generateQuarterData(code, dataList, 'total_cogs')

    dataList = sortedIncome[['end_date', 'oper_cost']]
    generateQuarterData(code, dataList, 'oper_cost')


# dataList format: [[date, value]]
def generateQuarterData(code, dataList, columnName):
    sqlTemplate = "update `stock`.`income` set `%s_qtr` = %f where `ts_code` = '%s' and `end_date` = '%s' and `report_type` = '1'"

    try:
        for index, row in dataList.iterrows():
            endDate = datetime.datetime.strptime(row['end_date'], "%Y%m%d")
            lastEndDate = None
            lastValue = 0
            quarterValue = 0

            if endDate.month == 3 and endDate.day == 31:
                quarterValue = row[columnName]
            elif endDate.month == 6 and endDate.day == 30:
                lastEndDate = '%d0331' % endDate.year
            elif endDate.month == 9 and endDate.day == 30:
                lastEndDate = '%d0630' % endDate.year
            elif endDate.month == 12 and endDate.day == 31:
                lastEndDate = '%d0930' % endDate.year

            if lastEndDate:
                lastRow = dataList[dataList['end_date'] == lastEndDate].to_numpy()
                if len(lastRow) > 0:
                    lastValue = lastRow[0][1]
                    quarterValue = row[columnName] - lastValue
            
            print("%s %d %d" % (row['end_date'], row[columnName], quarterValue))
            sql = sqlTemplate % (columnName, quarterValue, code, row['end_date'])
            engine.execute(sql)

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error("write to DB for generateQuarterRateOnData error on sql: %s" % sql)


if __name__ == "__main__":
    # 查询语句：select distinct ts_code from usstock.income; 
    stockdf = pd.read_csv("C:/project/Tushare/stock/income_code.csv")
    stockList = stockdf['ts_code'].to_numpy()

    # generateQuarterRate('002271.SZ')
    generateQuarterRate('600176.SH')

    # for code in stockList:SH600176
    #     FileLogger.info("running on code: %s" % code)
    #     generateQuarterRate(code)
