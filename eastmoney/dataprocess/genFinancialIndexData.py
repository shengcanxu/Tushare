# %%
# 用于生成三张报表中不存在的一些指标，如毛利率，现净比
# 生成表格的语句
# CREATE TABLE `financialindex` (
#   `SECUCODE` varchar(20),
#   `REPORT_DATE` varchar(20),
# `SECURITY_NAME_ABBR`  varchar(20),
#   UNIQUE KEY `idx_financialindex_SECUCODE_REPORT_DATE` (`SECUCODE`,`REPORT_DATE`)
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
import sys
sys.path.append("C:/project/Tushare")
import pandas as pd
import datetime
from sqlalchemy import create_engine
from helper.logger import FileLogger
import eastmoney.dataprocess.DBHelper as dataGetter
import eastmoney.dataprocess.processData as processor
import math


ENGINE = create_engine("mysql+pymysql://root:4401821211@localhost:3306/eastmoney?charset=utf8")


# %%
# 如果数据不存在，先insert一条记录
def addFinIndexBasicObject(basicRecord):
    sqlstr = "insert into `eastmoney`.`financialindex`(SECUCODE,SECURITY_NAME_ABBR,REPORT_DATE) values('%s','%s','%s')"
    sqlstr = sqlstr % (
        basicRecord["SECUCODE"],
        basicRecord["SECURITY_NAME_ABBR"],
        basicRecord["REPORT_DATE"]
    )
    ENGINE.execute(sqlstr)


# 根据jsonsql生成update sql并且执行
# jsonSql: {'SZ000001':{'2021-03-31':[[column1, value1],[column2,value2]]}}
def executeUpdateSql(jsonSql):
    for code in jsonSql.keys():
        for date in jsonSql[code].keys():
            columns = jsonSql[code][date]

            setStr = ""
            for column in columns:
                if len(setStr) == 0:
                    setStr = "set %s=%s" % (column[0], column[1])
                else:
                    setStr = setStr + ", %s=%s" % (column[0], column[1])
            if len(setStr) > 0:
                sql = "update `eastmoney`.`financialindex` %s where SECUCODE='%s' and REPORT_DATE='%s'" % (setStr, code, date)
                ENGINE.execute(sql)


# 生成jsondf
# datadf: [REPORT_DATE, value]
# jsonSql: {'SZ000001':{'2021-03-31':[[column1, value1],[column2,value2]]}}
def composeJsonSql(jsonSql, code, datadf):
    column = datadf.columns[1]

    if code not in jsonSql:
        jsonSql[code] = {}
    for index, item in datadf.iterrows():
        reportDate = item[0]
        value = item[1]
        if reportDate not in jsonSql[code]:
            jsonSql[code][reportDate] = []

        if not (value is None or math.isnan(value)):
            jsonSql[code][reportDate].append([column, str(value)])
    
    return jsonSql


# 获得income表的收入YOY数据
def gatherIncomeYoY(jsonSql, code, incomedf):
    dataGetter.createColunnIfNotExists('financialindex', 'TOTAL_INCOME_YOY', type='double')

    # 产生YOY数据并生成用于写入数据库的jsonsql
    datadf = incomedf[['REPORT_DATE', 'TOTAL_OPERATE_INCOME']]
    yoydf = processor.genYoYDatas(datadf)
    yoydf = yoydf.rename(columns={'TOTAL_OPERATE_INCOME': 'TOTAL_INCOME_YOY'})
    composeJsonSql(jsonSql, code, yoydf)


# 获得income表的毛利率
def gatherGrossProfit(jsonSql, code, incomedf):
    dataGetter.createColunnIfNotExists('financialindex', 'GROSS_PROFIT', type='double')
    
    def calcGrossProfit(x):
        return x[2] / float(x[1])

    datadf = incomedf[['REPORT_DATE', 'OPERATE_INCOME', 'OPERATE_COST']]
    for index, item in datadf.iterrows():
        datadf.loc[index, 'GROSS_PROFIT'] = 1.0 - item['OPERATE_COST'] / float(item['OPERATE_INCOME'])
    
    grofitProfit = datadf[['REPORT_DATE', 'GROSS_PROFIT']]
    composeJsonSql(jsonSql, code, grofitProfit)


# %% __main__
stockdf = pd.read_csv("C:/project/Tushare/eastmoney/codewithcompanytype.csv")
stockList = stockdf[['ts_code', 'companytype']].to_numpy()

stockList = [['SZ000002', 4]]

# add the base info into DB
for item in stockList:
    code = item[0]
    companyType = item[1]
    # need to process companyType 1-3
    if companyType != 4:
        continue

    incomedf = dataGetter.getDataFromIncome(code)
    # 过滤掉已经处理过的数据
    # existdf = dataGetter.getDataFromFinIndex(code=code)
    # lastDate = '1980-01-01'
    # if len(existdf) != 0:
    #     lastDate = existdf['REPORT_DATE'].sort_values(ascending=False)[0]
    # incomedf = incomedf[incomedf['REPORT_DATE'] > lastDate]

    # add the base info into DB
    # for index, income in incomedf.iterrows():
    #     addFinIndexBasicObject(income)

    FileLogger.info("running on code: %s" % code)
    try:
        
        jsonSql = {}

        gatherIncomeYoY(jsonSql, code, incomedf)
        gatherGrossProfit(jsonSql, code, incomedf)

        executeUpdateSql(jsonSql)

    except Exception as ex:
            FileLogger.error(ex)
            FileLogger.error("parse income error on code: %s" % code)
            time.sleep(3)

# %%
