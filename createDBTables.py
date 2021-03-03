# create database tables for all data storage

from sqlalchemy import create_engine

engine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/stock?charset=utf8")
createTableStrTemp = """CREATE TABLE `%s` (
  `trade_date` varchar(20) DEFAULT NULL,
  `ts_code` varchar(20) DEFAULT NULL,
  `open` double DEFAULT NULL,
  `high` double DEFAULT NULL,
  `low` double DEFAULT NULL,
  `close` double DEFAULT NULL,
  `pre_close` double DEFAULT NULL,
  `change` double DEFAULT NULL,
  `pct_chg` double DEFAULT NULL,
  `vol` double DEFAULT NULL,
  `amount` double DEFAULT NULL,
  UNIQUE KEY `idx_daily_trade_date_ts_code` (`ts_code`,`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""

# create daily table
for year in range(1990, 2022):
    tableName = "daily" + str(year)
    createTableStr = createTableStrTemp % tableName
    engine.execute(createTableStr)

# create weekly table
for year in ['199X', '200X', '201X', '202X']:
    tableName = "weekly" + year
    createTableStr = createTableStrTemp % tableName
    engine.execute(createTableStr)

# create monthly table
tableName = "monthly"
createTableStr = createTableStrTemp % tableName
engine.execute(createTableStr)