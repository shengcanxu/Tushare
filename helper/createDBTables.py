# create database tables for all data storage

from sqlalchemy import create_engine

engine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/stock?charset=utf8")
createDailyTableStrTemp = """CREATE TABLE `%s` (
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

createAdjustFactorTableStrTemp = """
CREATE TABLE `%s` (
  `ts_code` varchar(20),
  `trade_date` varchar(20),
  `adj_factor` double DEFAULT NULL,
  UNIQUE KEY `idx_adjustfactor_trade_date_ts_code` (`ts_code`,`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
"""

# # create daily table
# for num in range(0, 30):
#     tableName = "daily" + str(num)
#     createTableStr = createDailyTableStrTemp % tableName
#     engine.execute(createTableStr)

# # create weekly table
# for text in ['0', '1', '2', '3', '4', '5']:
#     tableName = "weekly" + text
#     createTableStr = createDailyTableStrTemp % tableName
#     engine.execute(createTableStr)

# # create monthly table
# tableName = "monthly"
# createTableStr = createTableStrTemp % tableName
# engine.execute(createTableStr)

# # create adjust factor table
# for num in range(0, 30):
#     tableName = "adjustfactor" + str(num)
#     createTableStr = createAdjustFactorTableStrTemp % tableName
#     engine.execute(createTableStr)