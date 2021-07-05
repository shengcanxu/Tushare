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
def gatherGrossProfitRate(jsonSql, code, incomedf):
    dataGetter.createColunnIfNotExists('financialindex', 'GROSS_PROFIT_RATE', type='double')

    datadf = incomedf[['REPORT_DATE', 'TOTAL_OPERATE_INCOME', 'OPERATE_COST']]
    for index, item in datadf.iterrows():
        datadf.loc[index, 'GROSS_PROFIT_RATE'] = 1.0 - item['OPERATE_COST'] / float(item['TOTAL_OPERATE_INCOME'])
    
    rate = datadf[['REPORT_DATE', 'GROSS_PROFIT_RATE']]
    composeJsonSql(jsonSql, code, rate)


# 获得净利率
def gatherNetProfitRate(jsonSql, code, incomedf):
    dataGetter.createColunnIfNotExists('financialindex', 'NETPROFIT_RATE', type='double')
    
    datadf = incomedf[['REPORT_DATE', 'TOTAL_OPERATE_INCOME', 'NETPROFIT']]
    for index, item in datadf.iterrows():
        datadf.loc[index, 'NETPROFIT_RATE'] = item['NETPROFIT'] / float(item['TOTAL_OPERATE_INCOME'])
    
    rate = datadf[['REPORT_DATE', 'NETPROFIT_RATE']]
    composeJsonSql(jsonSql, code, rate)


# 获得运营利润率
def gatherOperateProfitRate(jsonSql, code, incomedf):
    dataGetter.createColunnIfNotExists('financialindex', 'OPERATE_PROFIT_RATE', type='double')
    
    datadf = incomedf[['REPORT_DATE', 'TOTAL_OPERATE_INCOME', 'OPERATE_PROFIT']]
    for index, item in datadf.iterrows():
        datadf.loc[index, 'OPERATE_PROFIT_RATE'] = item['OPERATE_PROFIT'] / float(item['TOTAL_OPERATE_INCOME'])
    
    rate = datadf[['REPORT_DATE', 'OPERATE_PROFIT_RATE']]
    composeJsonSql(jsonSql, code, rate)


# 获得利润率
def gatherProfitRate(jsonSql, code, incomedf):
    dataGetter.createColunnIfNotExists('financialindex', 'PROFIT_RATE', type='double')
    
    datadf = incomedf[['REPORT_DATE', 'TOTAL_OPERATE_INCOME', 'TOTAL_PROFIT']]
    for index, item in datadf.iterrows():
        datadf.loc[index, 'PROFIT_RATE'] = item['TOTAL_PROFIT'] / float(item['TOTAL_OPERATE_INCOME'])
    
    rate = datadf[['REPORT_DATE', 'PROFIT_RATE']]
    composeJsonSql(jsonSql, code, rate)


# 获得营业税占收入比率
def gatherOperateTaxRate(jsonSql, code, incomedf):
    dataGetter.createColunnIfNotExists('financialindex', 'OPERATE_TAX_RATE', type='double')
    
    datadf = incomedf[['REPORT_DATE', 'TOTAL_OPERATE_INCOME', 'OPERATE_TAX_ADD']]
    for index, item in datadf.iterrows():
        datadf.loc[index, 'OPERATE_TAX_RATE'] = item['OPERATE_TAX_ADD'] / float(item['TOTAL_OPERATE_INCOME'])
    
    rate = datadf[['REPORT_DATE', 'OPERATE_TAX_RATE']]
    composeJsonSql(jsonSql, code, rate)


# 获得营销成本占收入比率
def gatherSalesRate(jsonSql, code, incomedf):
    dataGetter.createColunnIfNotExists('financialindex', 'SALES_RATE', type='double')
    
    datadf = incomedf[['REPORT_DATE', 'TOTAL_OPERATE_INCOME', 'SALE_EXPENSE']]
    for index, item in datadf.iterrows():
        datadf.loc[index, 'SALES_RATE'] = item['SALE_EXPENSE'] / float(item['TOTAL_OPERATE_INCOME'])
    
    rate = datadf[['REPORT_DATE', 'SALES_RATE']]
    composeJsonSql(jsonSql, code, rate)


# 获得管理成本占收入比率
def gatherManageRate(jsonSql, code, incomedf):
    dataGetter.createColunnIfNotExists('financialindex', 'MANAGE_RATE', type='double')
    
    datadf = incomedf[['REPORT_DATE', 'TOTAL_OPERATE_INCOME', 'MANAGE_EXPENSE']]
    for index, item in datadf.iterrows():
        datadf.loc[index, 'MANAGE_RATE'] = item['MANAGE_EXPENSE'] / float(item['TOTAL_OPERATE_INCOME'])
    
    rate = datadf[['REPORT_DATE', 'MANAGE_RATE']]
    composeJsonSql(jsonSql, code, rate)


# 获得财务成本占收入比率
def gatherFinanceRate(jsonSql, code, incomedf):
    dataGetter.createColunnIfNotExists('financialindex', 'FINANCE_RATE', type='double')
    
    datadf = incomedf[['REPORT_DATE', 'TOTAL_OPERATE_INCOME', 'FINANCE_EXPENSE']]
    for index, item in datadf.iterrows():
        datadf.loc[index, 'FINANCE_RATE'] = item['FINANCE_EXPENSE'] / float(item['TOTAL_OPERATE_INCOME'])
    
    rate = datadf[['REPORT_DATE', 'FINANCE_RATE']]
    composeJsonSql(jsonSql, code, rate)


# 获得研发成本占收入比率
def gatherResearchRate(jsonSql, code, incomedf):
    dataGetter.createColunnIfNotExists('financialindex', 'RESEARCH_RATE', type='double')
    
    datadf = incomedf[['REPORT_DATE', 'TOTAL_OPERATE_INCOME', 'RESEARCH_EXPENSE']]
    for index, item in datadf.iterrows():
        datadf.loc[index, 'RESEARCH_RATE'] = item['RESEARCH_EXPENSE'] / float(item['TOTAL_OPERATE_INCOME'])
    
    rate = datadf[['REPORT_DATE', 'RESEARCH_RATE']]
    composeJsonSql(jsonSql, code, rate)


# 获得所得税占收入比率
def gatherIncomeTaxRate(jsonSql, code, incomedf):
    dataGetter.createColunnIfNotExists('financialindex', 'INCOME_TAX_RATE', type='double')
    
    datadf = incomedf[['REPORT_DATE', 'TOTAL_OPERATE_INCOME', 'INCOME_TAX']]
    for index, item in datadf.iterrows():
        datadf.loc[index, 'INCOME_TAX_RATE'] = item['INCOME_TAX'] / float(item['TOTAL_OPERATE_INCOME'])
    
    rate = datadf[['REPORT_DATE', 'INCOME_TAX_RATE']]
    composeJsonSql(jsonSql, code, rate)



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

        # gatherIncomeYoY(jsonSql, code, incomedf)
        # gatherGrossProfitRate(jsonSql, code, incomedf)
        # gatherNetProfitRate(jsonSql, code, incomedf)
        # gatherOperateProfitRate(jsonSql, code, incomedf)
        # gatherProfitRate(jsonSql, code, incomedf)
        # gatherOperateTaxRate(jsonSql, code, incomedf)
        # gatherSalesRate(jsonSql, code, incomedf)
        # gatherManageRate(jsonSql, code, incomedf)
        # gatherFinanceRate(jsonSql, code, incomedf)
        # gatherResearchRate(jsonSql, code, incomedf)
        gatherIncomeTaxRate(jsonSql, code, incomedf)

        executeUpdateSql(jsonSql)

    except Exception as ex:
            FileLogger.error(ex)
            FileLogger.error("parse income error on code: %s" % code)
            time.sleep(3)

# %%
