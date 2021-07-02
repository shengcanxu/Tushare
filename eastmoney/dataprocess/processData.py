# %%
import sys
sys.path.append("C:/project/Tushare")
import pandas as pd
import datetime
from sqlalchemy import create_engine
from helper.logger import FileLogger
import eastmoney.dataprocess.getFinancailDataFromDB as dataGetter


# datadf: the data queried from database
def keepOnlyYearData(datadf):
    if 'REPORT_DATE' not in datadf.columns:
        return None
    else:
        return datadf[datadf['REPORT_DATE'].str.find('-12-31') != -1]


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


TEXTCOLUMNS = ['SECUCODE', 'SECURITY_NAME_ABBR', 'REPORT_DATE', 'REPORT_TYPE', 'UPDATE_DATE', 'CURRENCY', 'OPTION_TYPE']


# 数据库里面存储的数据是累加数据，用这个函数可以将数据变成单季度的数据
# 如果columns有值，就是仅将columns里面的列做转换，默认全部转换
def genQuarterDatas(datadf, columns=[]):
    if len(columns) == 0:
        columns = datadf.columns
    if 'REPORT_DATE' not in columns:
        FileLogger.error("REPORT_DATE must be in datadf!")

    newDF = pd.DataFrame({})
    for col in columns:
        if col in TEXTCOLUMNS:
            newDF[col] = datadf[col]
            continue
        df = datadf[['REPORT_DATE', col]]
        df = _genQuaterData(df)
        newDF[col] = df[col]

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


# 如果columns有值，就是仅将columns里面的列做转换，默认全部转换
# datadf is the accumulate value, will generate quarter data in this function
def genQoQDatas(datadf, columns=[]):
    if len(columns) == 0:
        columns = datadf.columns
    if 'REPORT_DATE' not in columns:
        FileLogger.error("REPORT_DATE must be in datadf!")
    quarterdf = genQuarterDatas(datadf)

    newDF = pd.DataFrame({})
    for col in columns:
        if col in TEXTCOLUMNS:
            newDF[col] = quarterdf[col]
            continue
        df = quarterdf[['REPORT_DATE', col]]
        df = _genQosData(df)
        newDF[col] = df[col]

    return newDF


def _genQosData(quarterdf):
    copydf = quarterdf.copy()
    for index, row in quarterdf.iterrows():
        date = datetime.datetime.strptime(row['REPORT_DATE'], "%Y-%m-%d")
        
        lastDate = None
        if date.month == 3 and date.day == 31:
            lastDate = '%d-12-31' % (date.year-1)
        elif date.month == 6 and date.day == 30:
            lastDate = '%d-03-31' % date.year
        elif date.month == 9 and date.day == 30:
            lastDate = '%d-06-30' % date.year
        elif date.month == 12 and date.day == 31:
            lastDate = '%d-09-30' % date.year
        lastRow = quarterdf[quarterdf['REPORT_DATE'] == lastDate].to_numpy()
        if len(lastRow) > 0 and lastRow[0][1]:
            copydf.iloc[index, 1] = row[1] / float(lastRow[0][1]) - 1
        else:
            copydf.iloc[index, 1] = None

    return copydf


# 如果columns有值，就是仅将columns里面的列做转换，默认全部转换
def genYoYDatas(datadf, columns=[]):
    if len(columns) == 0:
        columns = datadf.columns
    if 'REPORT_DATE' not in columns:
        FileLogger.error("REPORT_DATE must be in datadf!")

    newDF = pd.DataFrame({})
    for col in columns:
        if col in TEXTCOLUMNS:
            newDF[col] = datadf[col]
            continue
        df = datadf[['REPORT_DATE', col]]
        df = _genYoYData(df)
        newDF[col] = df[col]

    return newDF


# datadf一定是[REPORT_DATE, column]
def _genYoYData(datadf):
    copydf = datadf.copy()
    for index, row in datadf.iterrows():
        date = datetime.datetime.strptime(row['REPORT_DATE'], "%Y-%m-%d")
        
        lastDate = '%d-%02d-%02d' % (date.year-1, date.month, date.day)
        lastRow = datadf[datadf['REPORT_DATE'] == lastDate].to_numpy()
        if len(lastRow) > 0 and lastRow[0][1]:
            copydf.iloc[index, 1] = row[1] / float(lastRow[0][1]) - 1
        else:
            copydf.iloc[index, 1] = None

    return copydf


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


def mapIncomeColumnName(datadf):
    return _mapTitleName(datadf, 'income')


def mapBalanceColumnName(datadf):
    return _mapTitleName(datadf, 'balance')


def mapCashflowColumnName(datadf):
    return _mapTitleName(datadf, 'cashflow')


# 将dataframe里面的值用XX万，XX亿或者XX.X%来表示
def formatData4Show(datadf, percentColumns=[]):
    def formatFunc(x):
        if x >= 100000000:
            return "{:.2f}亿".format(x/100000000)
        elif x >= 10000:
            return "{:.1f}万".format(x/10000)
        else:
            return "{:.1f}".format(x)

    copydf = datadf.copy()
    for row in datadf.itertuples():
        for column in datadf.columns:
            if column in TEXTCOLUMNS:
                continue
            if column in percentColumns:
                copydf[column] = datadf[column].map(lambda x: "{:.2f}%".format(x*100))
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

