import sys
sys.path.append("C:/project/Tushare")
import pandas as pd
import datetime
from sqlalchemy import create_engine
from helper.logger import FileLogger


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

    return copydf

