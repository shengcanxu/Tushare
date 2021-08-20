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
import time


ENGINE = create_engine("mysql+pymysql://root:4401821211@localhost:3306/eastmoney?charset=utf8")


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
def getIncomeYoY(code, incomedf):
    datadf = incomedf[['TOTAL_OPERATE_INCOME']]
    yoydf = processor.genYoYDatas(datadf)
    yoydf = yoydf.rename(columns={'TOTAL_OPERATE_INCOME': 'TOTAL_INCOME_YOY'})
    return yoydf


# 获得income表的毛利率
def getGrossProfitRate(code, incomedf):
    datadf = incomedf
    for index, item in datadf.iterrows():
        datadf.loc[index, 'GROSS_PROFIT_RATE'] = 1.0 - item['OPERATE_COST'] / float(item['TOTAL_OPERATE_INCOME'])
    rate = datadf[['GROSS_PROFIT_RATE']]
    return rate


# 获得净利率
def getNetProfitRate(code, incomedf):
    datadf = incomedf
    for index, item in datadf.iterrows():
        datadf.loc[index, 'NETPROFIT_RATE'] = item['NETPROFIT'] / float(item['TOTAL_OPERATE_INCOME'])
    rate = datadf[['NETPROFIT_RATE']]
    return rate


# 获得运营利润率
def getOperateProfitRate(code, incomedf):
    datadf = incomedf
    for index, item in datadf.iterrows():
        datadf.loc[index, 'OPERATE_PROFIT_RATE'] = item['OPERATE_PROFIT'] / float(item['TOTAL_OPERATE_INCOME'])
    rate = datadf[['OPERATE_PROFIT_RATE']]
    return rate


# 获得利润率
def getProfitRate(code, incomedf):
    datadf = incomedf
    for index, item in datadf.iterrows():
        datadf.loc[index, 'PROFIT_RATE'] = item['TOTAL_PROFIT'] / float(item['TOTAL_OPERATE_INCOME'])
    rate = datadf[['PROFIT_RATE']]
    return rate


# 获得营业税占收入比率
def getOperateTaxRate(code, incomedf):
    datadf = incomedf
    for index, item in datadf.iterrows():
        datadf.loc[index, 'OPERATE_TAX_RATE'] = item['OPERATE_TAX_ADD'] / float(item['TOTAL_OPERATE_INCOME'])
    rate = datadf[['OPERATE_TAX_RATE']]
    return rate


# 获得营销成本占收入比率
def getSalesRate(code, incomedf):
    datadf = incomedf
    for index, item in datadf.iterrows():
        datadf.loc[index, 'SALES_RATE'] = item['SALE_EXPENSE'] / float(item['TOTAL_OPERATE_INCOME'])
    rate = datadf[['SALES_RATE']]
    return rate


# 获得管理成本占收入比率
def getManageRate(code, incomedf):
    datadf = incomedf
    for index, item in datadf.iterrows():
        datadf.loc[index, 'MANAGE_RATE'] = item['MANAGE_EXPENSE'] / float(item['TOTAL_OPERATE_INCOME'])
    rate = datadf[['MANAGE_RATE']]
    return rate


# 获得财务成本占收入比率
def getFinanceRate(code, incomedf):
    datadf = incomedf
    for index, item in datadf.iterrows():
        datadf.loc[index, 'FINANCE_RATE'] = item['FINANCE_EXPENSE'] / float(item['TOTAL_OPERATE_INCOME'])
    rate = datadf[['FINANCE_RATE']]
    return rate


# 获得研发成本占收入比率
def getResearchRate(code, incomedf):
    datadf = incomedf
    for index, item in datadf.iterrows():
        datadf.loc[index, 'RESEARCH_RATE'] = item['RESEARCH_EXPENSE'] / float(item['TOTAL_OPERATE_INCOME'])   
    rate = datadf[['RESEARCH_RATE']]
    return rate


# 获得所得税占收入比率
def getIncomeTaxRate(code, incomedf):
    datadf = incomedf
    for index, item in datadf.iterrows():
        datadf.loc[index, 'INCOME_TAX_RATE'] = item['INCOME_TAX'] / float(item['TOTAL_OPERATE_INCOME'])
    rate = datadf[['INCOME_TAX_RATE']]
    return rate


# 获得运营费用
def getOperateFee(code, incomedf):
    datadf = incomedf
    for index, item in datadf.iterrows():
        datadf.loc[index, 'OPERATE_FEE'] = item['RESEARCH_EXPENSE'] + item['FINANCE_EXPENSE'] + item['MANAGE_EXPENSE'] + item['SALE_EXPENSE']
    result = datadf[['OPERATE_FEE']]
    return result


# 获得运营费用占收入占比
def getOperateFeeRate(code, incomedf):
    operateFeedf = getOperateFee(code, incomedf)
    incomedf = incomedf[['TOTAL_OPERATE_INCOME']]
    datadf = pd.merge(operateFeedf, incomedf, how='left', on='REPORT_DATE')

    for index, item in datadf.iterrows():
        datadf.loc[index, 'OPERATE_FEE_RATE'] = item['OPERATE_FEE'] / float(item['TOTAL_OPERATE_INCOME'])   
    rate = datadf[['OPERATE_FEE_RATE']]
    return rate


# 获得资产周转率
def getAssetTurnoverRate(code, balancedf, incomedf):
    datadf = balancedf[['TOTAL_ASSETS']].copy()
    datadf = processor.genAvgData(datadf, columns=['TOTAL_ASSETS'], period='year')
    datadf['TOTAL_OPERATE_INCOME'] = incomedf['TOTAL_OPERATE_INCOME']
    datadf['ASSET_TURNOVER_RATE'] = datadf.apply(lambda x: x['TOTAL_OPERATE_INCOME'] / float(x['TOTAL_ASSETS']), axis=1)
    rate = datadf[['ASSET_TURNOVER_RATE']]
    return rate


# 获得资产负债率 Assets Liabilities Ratio
def getAssetLiabilityRate(code, balancedf):
    datadf = balancedf
    for index, item in datadf.iterrows():
        datadf.loc[index, 'ASSET_LIAB_RATE'] = item['TOTAL_LIABILITIES'] / float(item['TOTAL_ASSETS'])
    rate = datadf[['ASSET_LIAB_RATE']]
    return rate


# 获得权益乘数 equity multiplier
def getEquityMultiplier(code, balancedf):
    rate = getAssetLiabilityRate(code, balancedf)
    rate['EQUITY_MULTIPLIER'] = rate['ASSET_LIAB_RATE'].map(lambda x: 1.0/(1-x))
    return rate[['EQUITY_MULTIPLIER']]
    

# 获得净资产收益率ROE
def getROE(code, balancedf, incomedf):
    datadf = balancedf[['TOTAL_EQUITY']]
    datadf = processor.genAvgData(datadf, columns=['TOTAL_EQUITY'], period='year')
    datadf['NETPROFIT'] = incomedf['NETPROFIT']
    datadf['ROE'] = datadf.apply(lambda x: x['NETPROFIT'] / float(x['TOTAL_EQUITY']), axis=1)
    rate = datadf[['ROE']]
    return rate


# 获得ROE分析表格， 分析净利润率，权益乘数，和资产周转率
def getROEAnalyse(code, balancedf, incomedf):
    roe = getROE(code, balancedf, incomedf)
    profit = getProfitRate(code, incomedf)
    turnover = getAssetTurnoverRate(code, balancedf, incomedf)
    multiplier = getEquityMultiplier(code, balancedf)
    table = roe.copy()
    table['PROFIT_RATE'] = profit['PROFIT_RATE']
    table['ASSET_TURNOVER_RATE'] = turnover['ASSET_TURNOVER_RATE']
    table['EQUITY_MULTIPLIER'] = multiplier['EQUITY_MULTIPLIER']
    tableT = pd.DataFrame(table.values.T, index=table.columns, columns=table.index)  # 转置
    return tableT


# 获得资产收益率ROA
def getROA(code, balancedf, incomedf):
    datadf = balancedf[['TOTAL_ASSETS']]
    datadf = processor.genAvgData(datadf, columns=['TOTAL_ASSETS'], period='year')
    datadf['NETPROFIT'] = incomedf['NETPROFIT']
    datadf['ROA'] = datadf.apply(lambda x: x['NETPROFIT'] / float(x['TOTAL_ASSETS']), axis=1)
    rate = datadf[['ROA']]
    return rate


# 获得流动比率: 流动资产 / 流动负债
def getCurrentAssetLiabRate(code, balancedf):
    datadf = balancedf
    for index, item in datadf.iterrows():
        datadf.loc[index, 'CURRENT_ASSET_LIAB_RATE'] = item['TOTAL_CURRENT_ASSETS'] / float(item['TOTAL_CURRENT_LIAB'])
    rate = datadf[['CURRENT_ASSET_LIAB_RATE']]
    return rate


# 获得速动比率: (流动资产-存货) / 流动负债
def getNetAssetLiabRate(code, balancedf):
    datadf = balancedf
    for index, item in datadf.iterrows():
        datadf.loc[index, 'CURRENT_ASSET_LIAB_RATE'] = (item['TOTAL_CURRENT_ASSETS'] - item['INVENTORY']) / float(item['TOTAL_CURRENT_LIAB'])
    rate = datadf[['CURRENT_ASSET_LIAB_RATE']]
    return rate


# 获得应收账款周转率: 营业收入 / ((期初应收账款-期末应收账款)/2)
def getReceivableTurnoverRate(code, balancedf, incomedf):
    datadf = balancedf[['NOTE_ACCOUNTS_RECE']]
    datadf = processor.genAvgData(datadf, columns=['NOTE_ACCOUNTS_RECE'], period='year')
    datadf['TOTAL_OPERATE_INCOME'] = incomedf['TOTAL_OPERATE_INCOME']
    datadf['RECE_TURNOVER_RATE'] = datadf.apply(lambda x: x['TOTAL_OPERATE_INCOME'] / float(x['NOTE_ACCOUNTS_RECE']), axis=1)
    rate = datadf[['RECE_TURNOVER_RATE']]
    return rate


# 获得应付账款周转率: (期末.营业成本 + 期末.存货 - 期初.存货) / ((期末.应付票据及应付账款 + 期初.应付票据及应付账款) / 2)
def getPayableTurnoverRate(code, balancedf, incomedf):
    datadf = balancedf[['INVENTORY', 'NOTE_ACCOUNTS_PAYABLE']]
    datadf = processor.genAvgData(datadf, columns=['NOTE_ACCOUNTS_PAYABLE'], period='year')
    datadf = processor.genGrowNumber(datadf, columns=['INVENTORY'], period='year')
    datadf['TOTAL_OPERATE_COST'] = incomedf['TOTAL_OPERATE_COST']
    datadf['PAYABLE_TURNOVER_RATE'] = datadf.apply(lambda x: (x['TOTAL_OPERATE_COST'] + x['INVENTORY']) / float(x['NOTE_ACCOUNTS_PAYABLE']), axis=1)
    rate = datadf[['PAYABLE_TURNOVER_RATE']]
    return rate


if __name__ == "__main__":
    stockdf = pd.read_csv("C:/project/Tushare/eastmoney/codewithcompanytype.csv")
    stockList = stockdf[['ts_code', 'companytype']].to_numpy()

    stockList = [['SZ000002', 4]]
    # stockList = [['SZ300144', 4]]

    # add the base info into DB
    for item in stockList:
        code = item[0]
        companyType = item[1]
        # need to process companyType 1-3
        if companyType != 4:
            continue

        FileLogger.info("running on code: %s" % code)
        # try:
        incomedf = dataGetter.getDataFromIncome(code)
        incomedf = incomedf.set_index("REPORT_DATE")
        incomedf = processor.keepOnlyYearData(incomedf).fillna(0)
        balancedf = dataGetter.getDataFromBalance(code)
        balancedf = balancedf.set_index("REPORT_DATE")
        balancedf = processor.keepOnlyYearData(balancedf).fillna(0)

        # rate = getIncomeYoY(code, incomedf)
        # rate = getGrossProfitRate(code, incomedf)
        # rate = getNetProfitRate(code, incomedf)
        # rate = getOperateProfitRate(code, incomedf)
        # rate = getProfitRate(code, incomedf)
        # rate = getOperateTaxRate(code, incomedf)
        # rate = getSalesRate(code, incomedf)
        # rate = getManageRate(code, incomedf)
        # rate = getFinanceRate(code, incomedf)
        # rate = getResearchRate(code, incomedf)
        # rate = getIncomeTaxRate(code, incomedf)
        # rate = getOperateFee(code, incomedf)
        # rate = getOperateFeeRate(code, incomedf)
        # rate = getAssetTurnoverRate(code, balancedf, incomedf)
        # rate = getAssetLiabilityRate(code, balancedf)
        # rate = getEquityMultiplier(code, balancedf)
        # rate = getROE(code, balancedf, incomedf)
        # rate = getROA(code, balancedf, incomedf)
        # rate = getCurrentAssetLiabRate(code, balancedf)
        # rate = getNetAssetLiabRate(code, balancedf)
        # rate = getReceivableTurnoverRate(code, balancedf, incomedf)
        rate = getPayableTurnoverRate(code, balancedf, incomedf)
        # rate = getROEAnalyse(code, balancedf, incomedf)

        # formateddf = processor.formatData4Show(rate, percentColumns=rate.columns)
        # formateddf = processor.formatData4Show(rate)
        # print(formateddf)
        print(rate)

        # except Exception as ex:
        #         FileLogger.error(ex)
        #         FileLogger.error("parse income error on code: %s" % code)
        #         time.sleep(3)

