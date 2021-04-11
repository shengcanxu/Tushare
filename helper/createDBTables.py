# create database tables for all data storage

from sqlalchemy import create_engine

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

createDailyBasicTablesStrTemp = """
CREATE TABLE `%s` (
  `ts_code` varchar(20) DEFAULT NULL,
  `trade_date` varchar(20) DEFAULT NULL,
  `close` double DEFAULT NULL,
  `turnover_rate` double DEFAULT NULL,
  `turnover_rate_f` double DEFAULT NULL,
  `volume_ratio` double DEFAULT NULL,
  `pe` double DEFAULT NULL,
  `pe_ttm` double DEFAULT NULL,
  `pb` double DEFAULT NULL,
  `ps` double DEFAULT NULL,
  `ps_ttm` double DEFAULT NULL,
  `dv_ratio` double DEFAULT NULL,
  `dv_ttm` double DEFAULT NULL,
  `total_share` double DEFAULT NULL,
  `float_share` double DEFAULT NULL,
  `free_share` double DEFAULT NULL,
  `total_mv` double DEFAULT NULL,
  `circ_mv` double DEFAULT NULL,
  UNIQUE KEY `idx_dailybasic_trade_date_ts_code` (`ts_code`,`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""

createAdjustFactorTableStrTemp = """
CREATE TABLE `%s` (
  `ts_code` varchar(20),
  `trade_date` varchar(20),
  `adj_factor` double DEFAULT NULL,
  UNIQUE KEY `idx_adjustfactor_trade_date_ts_code` (`ts_code`,`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
"""

createHkDailyTableStrTemp = """
CREATE TABLE `%s` (
  `ts_code` varchar(20),
  `trade_date` varchar(20),
  `open` double DEFAULT NULL,
  `high` double DEFAULT NULL,
  `low` double DEFAULT NULL,
  `close` double DEFAULT NULL,
  `pre_close` double DEFAULT NULL,
  `change` double DEFAULT NULL,
  `pct_chg` double DEFAULT NULL,
  `vol` double DEFAULT NULL,
  `amount` double DEFAULT NULL,
  UNIQUE KEY `ix_hkdaily_ts_code_trade_date` (`ts_code`, `trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
"""

engine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/stock?charset=utf8")

# # create daily table
# for num in range(0, 30):
#     tableName = "daily" + str(num)
#     createTableStr = createDailyTableStrTemp % tableName
#     engine.execute(createTableStr)

# # create daily basic  table
# for num in range(0, 30):
#     tableName = "dailybasic" + str(num)
#     createTableStr = createDailyBasicTablesStrTemp % tableName
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

# # create  HK daily table  
# for num in range(0, 10):
#     tableName = "hkdaily" + str(num)
#     createTableStr = createHkDailyTableStrTemp % tableName
#     engine.execute(createTableStr)


# create index daily table
indexEngine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/indexdata?charset=utf8")

# # create daily table  
# for num in range(0, 30):
#     tableName = "daily" + str(num)
#     createTableStr = createDailyTableStrTemp % tableName
#     indexEngine.execute(createTableStr)

# # create weekly table
# for text in ['0', '1', '2', '3', '4', '5']:
#     tableName = "weekly" + text
#     createTableStr = createDailyTableStrTemp % tableName
#     indexEngine.execute(createTableStr)


createFundNavTableStrTemplate = """
CREATE TABLE `%s` (
  `ts_code` varchar(20),
  `end_date` varchar(20),
  `ann_date` varchar(20),
  `unit_nav` double DEFAULT NULL,
  `accum_nav` double DEFAULT NULL,
  `accum_div` varchar(20),
  `net_asset` double DEFAULT NULL,
  `total_netasset` double DEFAULT NULL,
  `adj_nav` double DEFAULT NULL,
  `update_flag` varchar(20),
  KEY `idx_nav_ts_code_end_date_adj_nav` (`ts_code`, `end_date`, `adj_nav`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
"""

createFundPortfolioTableStrTemp = """
CREATE TABLE `%s` (
  `ts_code` varchar(20),
  `ann_date` varchar(20),
  `symbol` varchar(20),
  `end_date` varchar(20),
  `mkv` double DEFAULT NULL,
  `amount` double DEFAULT NULL,
  `stk_mkv_ratio` double DEFAULT NULL,
  `stk_float_ratio` double DEFAULT NULL,
  UNIQUE KEY `idx_portfolio_ts_code_ann_date_symbol` (`ts_code`, `ann_date`, `symbol`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
"""

createFundDailyTablesStrTemp = """
CREATE TABLE `%s` (
  `ts_code` varchar(20),
  `trade_date` varchar(20),
  `pre_close` double DEFAULT NULL,
  `open` double DEFAULT NULL,
  `high` double DEFAULT NULL,
  `low` double DEFAULT NULL,
  `close` double DEFAULT NULL,
  `change` double DEFAULT NULL,
  `pct_chg` double DEFAULT NULL,
  `vol` double DEFAULT NULL,
  `amount` double DEFAULT NULL,
  KEY `idx_daily_ts_code_tradedate` (`ts_code`, `trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
"""

# fund engine
fundEngine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/fund?charset=utf8")

# # create nav table  
# for num in range(0, 3):
#     tableName = "nav" + str(num)
#     createTableStr = createFundNavTableStrTemplate % tableName
#     fundEngine.execute(createTableStr)

# # create portfolio table  
# for num in range(0, 3):
#     tableName = "portfolio" + str(num)
#     createTableStr = createFundPortfolioTableStrTemp % tableName
#     fundEngine.execute(createTableStr)

# # create fund daily table  
# for num in range(0, 3):
#     tableName = "daily" + str(num)
#     createTableStr = createFundDailyTablesStrTemp % tableName
#     fundEngine.execute(createTableStr)


createUsStockDailyTableStrTemp = """
CREATE TABLE `%s` (
  `ts_code` varchar(20), 
  `trade_date` varchar(20),
  `volume` bigint DEFAULT NULL,
  `open` double DEFAULT NULL,
  `high` double DEFAULT NULL,
  `low` double DEFAULT NULL,
  `close` double DEFAULT NULL,
  `chg` double DEFAULT NULL,
  `percent` double DEFAULT NULL,
  `turnoverrate` double DEFAULT NULL,
  `amount` double DEFAULT NULL,
  `pe` double DEFAULT NULL,
  `pb` double DEFAULT NULL,
  `ps` double DEFAULT NULL,
  `pcf` double DEFAULT NULL,
  `market_capital` double DEFAULT NULL,
  UNIQUE KEY `ix_daily_ts_code_trade_date` (`ts_code`, `trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
"""

# us stock engine
usEngine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/usstock?charset=utf8")

# # create us stock daily table  
# for num in range(0, 30):
#     tableName = "daily" + str(num)
#     createTableStr = createUsStockDailyTableStrTemp % tableName
#     usEngine.execute(createTableStr)
