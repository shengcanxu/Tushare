# 计算利润表 各种比率 并保存到incomerate 表格
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
    
    # dataList = sortedIncome[['end_date', 'total_revenue']].to_numpy()
    # generateQuarterRateOnData(code, dataList, 'total_revenue')
    # generateQuarterYRateOnData(code, dataList, 'total_revenue')

    # dataList = sortedIncome[['end_date', 'revenue']].to_numpy()
    # generateQuarterRateOnData(code, dataList, 'revenue')
    # generateQuarterYRateOnData(code, dataList, 'revenue')

    # dataList = sortedIncome[['end_date', 'int_income']].to_numpy()
    # generateQuarterRateOnData(code, dataList, 'int_income')
    # generateQuarterYRateOnData(code, dataList, 'int_income')

    dataList = sortedIncome[['end_date', 'fv_value_chg_gain']].to_numpy()
    generateQuarterRateOnData(code, dataList, 'fv_value_chg_gain')
    # generateQuarterYRateOnData(code, dataList, 'fv_value_chg_gain')

    # dataList = sortedIncome[['end_date', 'operate_profit']].to_numpy()
    # generateQuarterRateOnData(code, dataList, 'operate_profit')
    # generateQuarterYRateOnData(code, dataList, 'operate_profit')


# dataList format: [[date, value]]
def generateQuarterRateOnData(code, dataList, columnName):
    sqlTemplate = "update `stock`.`incomerate` set `%s_rate` = %f where `ts_code` = '%s' and `end_date` = '%s' and `report_type` = '1'"

    lastDate = datetime.datetime.strptime('19900101', "%Y%m%d")
    lastValue = 1
    lastAcculatedValue = 0
    rate = 0
    try:
        for item in dataList:
            date = item[0]
            acculatedValue = item[1]
            if acculatedValue is None or math.isnan(acculatedValue):
                continue
            value = acculatedValue - lastAcculatedValue

            endDate = datetime.datetime.strptime(date, "%Y%m%d")
            delta = endDate - lastDate
            if delta > datetime.timedelta(days=135):
                # do nothing, rate should be none
                pass
            else:
                # 计算季度比值，要先算出本季度的营收，income报表中都是累加的季度收益
                rate = (value / lastValue - 1) * 100
                sql = sqlTemplate % (columnName, rate, code, date)
                engine.execute(sql)

            print("%d %d %d %d %f" % (lastAcculatedValue, acculatedValue, lastValue, value, rate))
            lastDate = endDate
            lastValue = value
            if endDate.month == 12:
                lastAcculatedValue = 0
            else:
                lastAcculatedValue = acculatedValue

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error("write to DB for generateQuarterRateOnData error on sql: %s" % sql)


# dataList format: [[date, value]]
def generateQuarterYRateOnData(code, dataList, columnName):
    sqlTemplate = "update `stock`.`incomerate` set `%s_yrate` = %f where `ts_code` = '%s' and `end_date` = '%s' and `report_type` = '1'"

    lastAcculatedValue = 1
    historyValues = {}
    rate = 0
    try:
        for item in dataList:
            date = item[0]
            acculatedValue = item[1]
            if acculatedValue is None:
                continue

            # 计算季度比值，要先算出本季度的营收，income报表中都是累加的季度收益
            value = acculatedValue - lastAcculatedValue
            historyValues[date] = value

            endDate = datetime.datetime.strptime(date, "%Y%m%d")
            lastYearDate = datetime.datetime(year=endDate.year-1, month=endDate.month, day=endDate.day)
            lastYear = lastYearDate.strftime("%Y%m%d")
            lastYearValue = historyValues[lastYear] if lastYear in historyValues else None
            
            if lastYearValue:
                rate = (value / lastYearValue - 1) * 100
                sql = sqlTemplate % (columnName, rate, code, date)
                engine.execute(sql)
                # print("%s %d %d %d %d %f" % (date, lastAcculatedValue, acculatedValue, lastYearValue, value, rate))
            
            if endDate.month == 12:
                lastAcculatedValue = 0
            else:
                lastAcculatedValue = acculatedValue

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error("write to DB for generateQuarterYRateOnData error on sql: %s" % sql)


if __name__ == "__main__":
    # 查询语句：select distinct ts_code from usstock.income; 
    stockdf = pd.read_csv("C:/project/Tushare/stock/income_code.csv")
    stockList = stockdf['ts_code'].to_numpy()

    # generateQuarterRate('002271.SZ')
    generateQuarterRate('600176.SH')

    # for code in stockList:SH600176
    #     FileLogger.info("running on code: %s" % code)
    #     generateQuarterRate(code)
