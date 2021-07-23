# %%
import sys
sys.path.append("C:/project/Tushare")
import pandas as pd
import datetime
from sqlalchemy import create_engine
from helper.logger import FileLogger
import eastmoney.dataprocess.DBHelper as dataGetter
import math


TEXTCOLUMNS = ['SECUCODE', 'SECURITY_NAME_ABBR', 'REPORT_DATE', 'REPORT_TYPE', 'UPDATE_DATE', 'CURRENCY', 'OPTION_TYPE']


# 时间增加1年， 仅内部使用
def _add1Year(x):
    date = datetime.datetime.strptime(x, "%Y-%m-%d")
    return '%d-%02d-%02d' % (date.year+1, date.month, date.day)


# 时间增加1Q， 仅内部使用
def _add1Quarter(x):
    date = datetime.datetime.strptime(x, "%Y-%m-%d")
    lastDate = '1990-03-31'
    if date.month == 3 and date.day == 31:
        lastDate = '%d-06-30' % date.year
    elif date.month == 6 and date.day == 30:
        lastDate = '%d-09-30' % date.year
    elif date.month == 9 and date.day == 30:
        lastDate = '%d-12-31' % date.year
    elif date.month == 12 and date.day == 31:
        lastDate = '%d-03-31' % (date.year+1)
    return lastDate


def _constructPairDf(datadf, newDF, columns=[]):
    if len(columns) == 0:
        for col in datadf.columns:
            if col not in TEXTCOLUMNS:
                columns.append(col)
    if 'REPORT_DATE' not in columns:
        FileLogger.error("REPORT_DATE must be in datadf!")

    dfs = []
    for col in datadf.columns:
        if col not in columns:
            newDF[col] = datadf[col]
        else:
            dfs.append(datadf[['REPORT_DATE', col]])
    return dfs


# 仅保留年度数据
# datadf: the data queried from database
def keepOnlyYearData(datadf):
    if 'REPORT_DATE' not in datadf.columns:
        return None
    else:
        return datadf[datadf['REPORT_DATE'].str.find('-12-31') != -1]


# 仅保留指定的季度的数据
# datadf: the data queried from database, quarter: the number of quarter, Q1 = 1
def keepOnlyQuarterData(datadf, quarter):
    if 'REPORT_DATE' not in datadf.columns:
        return None
    if quarter == 1:
        return datadf[datadf['REPORT_DATE'].str.find('-03-31') != -1]
    elif quarter == 2:
        return datadf[datadf['REPORT_DATE'].str.find('-06-30') != -1]
    elif quarter == 3:
        return datadf[datadf['REPORT_DATE'].str.find('-09-30') != -1]
    elif quarter == 4:
        return datadf[datadf['REPORT_DATE'].str.find('-12-31') != -1]
    else:
        FileLogger.error("error quarter parameter!")
        return None


# 数据库里面存储的数据是累加数据，用这个函数可以将数据变成单季度的数据
# 如果columns有值，就是仅将columns里面的列做转换，默认全部转换
def genQuarterDatas(datadf, columns=[]):
    newDF = pd.DataFrame({})
    dfs = _constructPairDf(datadf, newDF, columns=columns)
    for df in dfs:
        genDf = _genQuaterData(df)
        newDF[genDf.columns[1]] = genDf[genDf.columns[1]]
    return newDF


# datadf一定是[REPORT_DATE, column]
def _genQuaterData(datadf):
    copydf = datadf.copy()
    for index, row in datadf.iterrows():
        date = datetime.datetime.strptime(row['REPORT_DATE'], "%Y-%m-%d")
        
        lastDate = None
        if date.month == 3 and date.day == 31:
            continue
        elif date.month == 6 and date.day == 30:
            lastDate = '%d-03-31' % date.year
        elif date.month == 9 and date.day == 30:
            lastDate = '%d-06-30' % date.year
        elif date.month == 12 and date.day == 31:
            lastDate = '%d-09-30' % date.year
        lastRow = datadf[datadf['REPORT_DATE'] == lastDate].to_numpy()
        if len(lastRow) > 0:
            copydf.iloc[index, 1] = row[1] - lastRow[0][1]
        else:
            copydf.iloc[index, 1] = None
    return copydf


# 产生数据的季度同比增长比例
# 如果columns有值，就是仅将columns里面的列做转换，默认全部转换
# datadf is the accumulate value, will generate quarter data in this function
def genQoQDatas(datadf, columns=[]):
    return genYoYQoQData(datadf, columns=columns, period='quarter')


# 产生数据的年同比增长比例
# 如果columns有值，就是仅将columns里面的列做转换，默认全部转换
def genYoYDatas(datadf, columns=[]):
    return genYoYQoQData(datadf, columns=columns, period='year')


def genYoYQoQData(datadf, columns=[], period='year'):
    newDF = pd.DataFrame({})
    dfs = _constructPairDf(datadf, newDF, columns=columns)
    for df in dfs:
        genDf = _genYoYQoQData(df, period=period)
        newDF[genDf.columns[1]] = genDf[genDf.columns[1]]
    return newDF


# datadf一定是[REPORT_DATE, column]
def _genYoYQoQData(datadf, period='year'):
    def func(x):
        if x[2] is None or x[2] == 0 or math.isnan(x[2]):
            return None
        else:
            return x[1] / float(x[2]) - 1
    datadf2 = datadf.copy()
    datadf2.columns = ['REPORT_DATE', 'LAST']
    if period == 'year':
        datadf2['REPORT_DATE'] = datadf2['REPORT_DATE'].map(_add1Year)
    else:
        datadf2['REPORT_DATE'] = datadf2['REPORT_DATE'].map(_add1Quarter)
    mergedf = pd.merge(datadf, datadf2, how='left', on='REPORT_DATE')
    mergedf[mergedf.columns[1]] = mergedf.apply(func, axis=1)
    return mergedf.iloc[:, 0:2]


# 计算年/季度增长量：期末值 - 期初值. period= 'year' / 'quarter'
def genGrowNumber(datadf, columns=[], period='year'):
    newDF = pd.DataFrame({})
    dfs = _constructPairDf(datadf, newDF, columns=columns)
    for df in dfs:
        genDf = _genGrowNumber(df, period=period)
        newDF[genDf.columns[1]] = genDf[genDf.columns[1]]
    return newDF


def _genGrowNumber(datadf, period='year'):
    datadf2 = datadf.copy()
    datadf2.columns = ['REPORT_DATE', 'LAST']
    if period == 'year':
        datadf2['REPORT_DATE'] = datadf2['REPORT_DATE'].map(_add1Year)
    else:
        datadf2['REPORT_DATE'] = datadf2['REPORT_DATE'].map(_add1Quarter)
    mergedf = pd.merge(datadf, datadf2, how='left', on='REPORT_DATE')
    mergedf[mergedf.columns[1]] = mergedf.apply(lambda x: x[1] - x[2], axis=1)
    return mergedf.iloc[:, 0:2]


# 计算年期间平均值(期初值+期末值)/2
def genYearAvgData(datadf, columns=[], period='year'):
    newDF = pd.DataFrame({})
    dfs = _constructPairDf(datadf, newDF, columns=columns)
    for df in dfs:
        genDf = _genYearAvgData(df, period=period)
        newDF[genDf.columns[1]] = genDf[genDf.columns[1]]
    return newDF


def _genYearAvgData(datadf, period='year'):
    datadf2 = datadf.copy()
    datadf2.columns = ['REPORT_DATE', 'LAST']
    if period == 'year':
        datadf2['REPORT_DATE'] = datadf2['REPORT_DATE'].map(_add1Year)
    else:
        datadf2['REPORT_DATE'] = datadf2['REPORT_DATE'].map(_add1Quarter)
    mergedf = pd.merge(datadf, datadf2, how='left', on='REPORT_DATE')
    mergedf[mergedf.columns[1]] = mergedf.apply(lambda x: (x[1]+x[2])/2.0, axis=1)
    return mergedf.iloc[:, 0:2]


# 将table的名字变成中文名
def _mapTitleName(datadf, table='income'):
    sql = "select * from `eastmoney`.`columnname` where `table`='%s'" % table
    columnNames = dataGetter._queryFromDB(sql)
    columnNames = columnNames[['column', 'name']].set_index('column')

    # create column mapping
    mapping = {}
    for col in datadf.columns:
        mapping[col] = columnNames.loc[col, 'name']
    # change the column name
    datadfC = datadf.rename(columns=mapping)
    datadfC = datadfC.set_index('报告时间')
    return datadfC


# 将income table的名字变成中文名
def mapIncomeColumnName(datadf):
    return _mapTitleName(datadf, 'income')


# 将balance table的名字变成中文名
def mapBalanceColumnName(datadf):
    return _mapTitleName(datadf, 'balance')


# 将cashflow table的名字变成中文名
def mapCashflowColumnName(datadf):
    return _mapTitleName(datadf, 'cashflow')


# 将dataframe里面的值用XX万，XX亿或者XX.X%来表示
def formatData4Show(datadf, percentColumns=[]):
    def formatFunc(x):
        if x is None or math.isnan(x):
            return ""
        elif x >= 100000000:
            return "{:.2f}亿".format(x/100000000)
        elif x >= 10000:
            return "{:.1f}万".format(x/10000)
        else:
            return "{:.2f}".format(x)

    def formatPercent(x):
        if x is None or math.isnan(x):
            return ""
        else:
            return "{:.2f}%".format(x*100)

    copydf = datadf.copy()
    for row in datadf.itertuples():
        for column in datadf.columns:
            if column in TEXTCOLUMNS:
                continue
            if column in percentColumns:
                copydf[column] = datadf[column].map(formatPercent)
            else:
                copydf[column] = datadf[column].map(formatFunc)
                
    return copydf



# %% main function
datadf = dataGetter.getDataFromIncome('SZ000002', ['REPORT_DATE', 'TOTAL_OPERATE_COST', 'TOTAL_OPERATE_INCOME'])
print(datadf)
# newDF = genQuarterDatas(datadf)
newDF = formatData4Show(genYoYDatas(datadf), percentColumns=['TOTAL_OPERATE_COST','TOTAL_OPERATE_INCOME'])
# newDF = mapIncomeColumnName(datadf)
# newDF = mapIncomeColumnName(formatData4Show(datadf))
# newDF = formatData4Show(genQoQDatas(datadf), percentColumns=['TOTAL_OPERATE_COST','TOTAL_OPERATE_INCOME'])

print(newDF)

# %%
datadf

# %%
newdf = genYoYDatas(datadf)
newdf

# %%
a= genGrowNumber(datadf, period='quarter')
a
# %%
