##
# 从公告中读取信息并下载附件（如有）
##
# %% 
import sys
sys.path.append("C:/project/Tushare")
from requests_html import HTMLSession
from helper.logger import FileLogger
import time
import json
import pandas as pd


# %%
# if __name__ == "__main__":
from eastmoney.getStockNotices import getJsonFromFile
