from sqlalchemy import create_engine
import pandas as pd

engine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/stock?charset=utf8")
dailyTemplate = """
insert into daily%i select * from daily%i where substring(`ts_code`, 5,2) = '%s';
"""

adjustFactorTemplate = """
insert into adjustfactor%i select * from adjustfactor%i where substring(`ts_code`, 5,2) = '%s';
"""

weeklytemplate = """
insert into weekly%i select * from weekly%s where substring(`ts_code`, 5,2) = '%s';
"""

# for year in range(1990, 2022):
#     for tail in range(0,100):
#         r = tail % 30
#         text = str(tail) if tail >=10 else '0' + str(tail) 
#         print(dailyTemplate % (r, year, text))

# # run the output sql string in mysql

for year in ['199x', '200x', '201x', '202x']:
    for tail in range(0,100):
        r = tail % 6
        text = str(tail) if tail >=10 else '0' + str(tail) 
        print(weeklytemplate % (r, year, text))

# run the output sql string in mysql
