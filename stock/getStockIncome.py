# 利润表
# 名称	类型	默认显示	描述
# ts_code	str	Y	TS代码
# ann_date	str	Y	公告日期
# f_ann_date	str	Y	实际公告日期
# end_date	str	Y	报告期
# report_type	str	Y	报告类型 1合并报表 2单季合并 3调整单季合并表 4调整合并报表 5调整前合并报表 6母公司报表 7母公司单季表 8 母公司调整单季表 9母公司调整表 10母公司调整前报表 11调整前合并报表 12母公司调整前报表
# comp_type	str	Y	公司类型(1一般工商业2银行3保险4证券)
# basic_eps	float	Y	基本每股收益
# diluted_eps	float	Y	稀释每股收益
# total_revenue	float	Y	营业总收入
# revenue	float	Y	营业收入
# int_income	float	Y	利息收入
# prem_earned	float	Y	已赚保费
# comm_income	float	Y	手续费及佣金收入
# n_commis_income	float	Y	手续费及佣金净收入
# n_oth_income	float	Y	其他经营净收益
# n_oth_b_income	float	Y	加:其他业务净收益
# prem_income	float	Y	保险业务收入
# out_prem	float	Y	减:分出保费
# une_prem_reser	float	Y	提取未到期责任准备金
# reins_income	float	Y	其中:分保费收入
# n_sec_tb_income	float	Y	代理买卖证券业务净收入
# n_sec_uw_income	float	Y	证券承销业务净收入
# n_asset_mg_income	float	Y	受托客户资产管理业务净收入
# oth_b_income	float	Y	其他业务收入
# fv_value_chg_gain	float	Y	加:公允价值变动净收益
# invest_income	float	Y	加:投资净收益
# ass_invest_income	float	Y	其中:对联营企业和合营企业的投资收益
# forex_gain	float	Y	加:汇兑净收益
# total_cogs	float	Y	营业总成本
# oper_cost	float	Y	减:营业成本
# int_exp	float	Y	减:利息支出
# comm_exp	float	Y	减:手续费及佣金支出
# biz_tax_surchg	float	Y	减:营业税金及附加
# sell_exp	float	Y	减:销售费用
# admin_exp	float	Y	减:管理费用
# fin_exp	float	Y	减:财务费用
# assets_impair_loss	float	Y	减:资产减值损失
# prem_refund	float	Y	退保金
# compens_payout	float	Y	赔付总支出
# reser_insur_liab	float	Y	提取保险责任准备金
# div_payt	float	Y	保户红利支出
# reins_exp	float	Y	分保费用
# oper_exp	float	Y	营业支出
# compens_payout_refu	float	Y	减:摊回赔付支出
# insur_reser_refu	float	Y	减:摊回保险责任准备金
# reins_cost_refund	float	Y	减:摊回分保费用
# other_bus_cost	float	Y	其他业务成本
# operate_profit	float	Y	营业利润
# non_oper_income	float	Y	加:营业外收入
# non_oper_exp	float	Y	减:营业外支出
# nca_disploss	float	Y	其中:减:非流动资产处置净损失
# total_profit	float	Y	利润总额
# income_tax	float	Y	所得税费用
# n_income	float	Y	净利润(含少数股东损益)
# n_income_attr_p	float	Y	净利润(不含少数股东损益)
# minority_gain	float	Y	少数股东损益
# oth_compr_income	float	Y	其他综合收益
# t_compr_income	float	Y	综合收益总额
# compr_inc_attr_p	float	Y	归属于母公司(或股东)的综合收益总额
# compr_inc_attr_m_s	float	Y	归属于少数股东的综合收益总额
# ebit	float	Y	息税前利润
# ebitda	float	Y	息税折旧摊销前利润
# insurance_exp	float	Y	保险业务支出
# undist_profit	float	Y	年初未分配利润
# distable_profit	float	Y	可分配利润
# update_flag	str	N	更新标识，0未修改1更正过
# 主要报表类型说明

# 代码	类型	说明
# 1	合并报表	上市公司最新报表（默认）
# 2	单季合并	单一季度的合并报表
# 3	调整单季合并表	调整后的单季合并报表（如果有）
# 4	调整合并报表	本年度公布上年同期的财务报表数据，报告期为上年度
# 5	调整前合并报表	数据发生变更，将原数据进行保留，即调整前的原数据
# 6	母公司报表	该公司母公司的财务报表数据
# 7	母公司单季表	母公司的单季度表
# 8	母公司调整单季表	母公司调整后的单季表
# 9	母公司调整表	该公司母公司的本年度公布上年同期的财务报表数据
# 10	母公司调整前报表	母公司调整之前的原始财务报表数据
# 11	调整前合并报表	调整之前合并报表原数据
# 12	母公司调整前报表	母公司报表发生变更前保留的原数据


import tushare as ts
from sqlalchemy import create_engine
import time
import pandas as pd

ts.set_token('803f1548c1f25bf44c56644e4527a6d8cd3dbd8517e7c59e3aa1f6d0')
pro = ts.pro_api()
engine = create_engine(
    "mysql+pymysql://root:4401821211@localhost:3306/stock?charset=utf8")


def getIncomeOnCode(tsCode, tushare, dbEngine):
    try:
        stockIncome = tushare.income(ts_code=tsCode, start_date='20210228', end_date='20210722')
        df = stockIncome.drop_duplicates(['ts_code', 'end_date', 'report_type'])
        df = df.set_index(["ts_code", "end_date"])

        tableName = "income"
        df.to_sql(name=tableName, con=dbEngine, if_exists="append")
        print("get Income data successfully on code: %s" % tsCode)

    except Exception as ex:
        print(ex)
        print("get Income data error on code: %s" % tsCode)


if __name__ == "__main__":
    sqlstr = "SELECT ts_code FROM stock.stockdata"
    stockList = pd.read_sql_query(sqlstr, con=engine).to_numpy()
    for tsCode in stockList:
        code = tsCode[0]
        getIncomeOnCode(code, pro, engine)
        time.sleep(1.5) # make sure less than 60 query per minute
