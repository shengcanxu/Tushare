# %%
import pandas as pd
import json


# %%
stockdf = pd.read_csv("C:/project/Tushare/eastmoney/codewithcompanytype.csv")
# stockdf = stockdf[stockdf.companytype == 4]
stockList = stockdf['ts_code'].to_numpy()
stockList

# %% 
len(stockList)

# %%
def readFile(filePath):
    try:
        fp = open(filePath, 'r')
        content = fp.read()
        return content
    except Exception as ex:
        return False

# %%
stat = {}

for code in stockList:
    print(code)
    path = "C:/project/stockdata/\EastMoneyBalance/%s.json" % code
    text = readFile(path)
    if text:
        jsonObjects = json.loads(text)
        for object in jsonObjects:
            for key in object.keys():
                if key in stat:
                    stat[key] = stat[key] + 1
                else:
                    stat[key] = 1

print(stat)

# %%
len(stat.keys())

# %%
notnull = {}

for code in stockList:
    print(code)
    path = "C:/project/stockdata/\EastMoneyBalance/%s.json" % code
    text = readFile(path)
    if text:
        jsonObjects = json.loads(text)
        for object in jsonObjects:
            for key in object.keys():
                if key not in notnull:
                    notnull[key] = 0
                if object[key] is not None:
                    notnull[key] = notnull[key] + 1

print(notnull)

# %%
