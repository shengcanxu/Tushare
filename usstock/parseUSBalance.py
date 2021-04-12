import pandas as pd
from helper.logger import FileLogger
from sqlalchemy import create_engine
import time
import json


usEngine = create_engine("mysql+pymysql://root:4401821211@localhost:3306/usstock?charset=utf8")

BASE_COLUMNS = ('ts_code', 'annual_settle_date', 'currency', 'currency_name',
                'last_report_name', 'org_type', 'quote_name', 'sas',
                'statuses', 'tip')

COLUMNS = (
    "ts_code",
    "report_date",
    "report_name",
    "ctime",
    "report_type_code",
    "report_annual",
    "sd",
    "ed",
    "total_assets",      "total_assets_rate",
    "total_liab",      "total_liab_rate",
    "asset_liab_ratio",      "asset_liab_ratio_rate",
    "net_property_plant_and_equip",      "net_property_plant_and_equip_rate",
    "total_assets_special_subject",      "total_assets_special_subject_rate",
    "lt_debt",      "lt_debt_rate",
    "total_liab_si",      "total_liab_si_rate",
    "preferred_stock",      "preferred_stock_rate",
    "common_stock",      "common_stock_rate",
    "add_paid_in_capital",      "add_paid_in_capital_rate",
    "retained_earning",      "retained_earning_rate",
    "treasury_stock",      "treasury_stock_rate",
    "accum_othr_compre_income",      "accum_othr_compre_income_rate",
    "total_holders_equity_si",      "total_holders_equity_si_rate",
    "total_holders_equity",      "total_holders_equity_rate",
    "minority_interest",      "minority_interest_rate",
    "total_equity_special_subject",      "total_equity_special_subject_rate",
    "total_equity",      "total_equity_rate",
    "cce",      "cce_rate",
    "st_invest",      "st_invest_rate",
    "total_cash",      "total_cash_rate",
    "net_receivables",      "net_receivables_rate",
    "inventory",      "inventory_rate",
    "dt_assets_current_assets",      "dt_assets_current_assets_rate",
    "prepaid_expense",      "prepaid_expense_rate",
    "current_assets_special_subject",      "current_assets_special_subject_rate",
    "total_current_assets",      "total_current_assets_rate",
    "gross_property_plant_and_equip",      "gross_property_plant_and_equip_rate",
    "accum_depreciation",      "accum_depreciation_rate",
    "equity_and_othr_invest",      "equity_and_othr_invest_rate",
    "goodwill",      "goodwill_rate",
    "net_intangible_assets",      "net_intangible_assets_rate",
    "accum_amortization",      "accum_amortization_rate",
    "dt_assets_noncurrent_assets",      "dt_assets_noncurrent_assets_rate",
    "nca_si",      "nca_si_rate",
    "total_noncurrent_assets",      "total_noncurrent_assets_rate",
    "st_debt",      "st_debt_rate",
    "accounts_payable",      "accounts_payable_rate",
    "income_tax_payable",      "income_tax_payable_rate",
    "accrued_liab",      "accrued_liab_rate",
    "deferred_revenue_current_liab",      "deferred_revenue_current_liab_rate",
    "current_liab_si",      "current_liab_si_rate",
    "total_current_liab",      "total_current_liab_rate",
    "deferred_tax_liab",      "deferred_tax_liab_rate",
    "dr_noncurrent_liab",      "dr_noncurrent_liab_rate",
    "noncurrent_liab_si",      "noncurrent_liab_si_rate",
    "total_noncurrent_liab",      "total_noncurrent_liab_rate",
    "loan",      "loan_rate",
    "fixed_maturity_sec",      "fixed_maturity_sec_rate",
    "equity_sec",      "equity_sec_rate",
    "td_sec",      "td_sec_rate",
    "othr_invest",      "othr_invest_rate",
    "total_invest",      "total_invest_rate",
    "accrued_invest_income",      "accrued_invest_income_rate",
    "premiums_and_othr_receivables",      "premiums_and_othr_receivables_rate",
    "rein_assets",      "rein_assets_rate",
    "deferred_policy_obtain_cost",      "deferred_policy_obtain_cost_rate",
    "di_tax",      "di_tax_rate",
    "separate_account_assets",      "separate_account_assets_rate",
    "futures_policy_benefits",      "futures_policy_benefits_rate",
    "policyholder_funds",      "policyholder_funds_rate",
    "unearned_premium",      "unearned_premium_rate",
    "reinsurance_liab",      "reinsurance_liab_rate",
    "tax_payable",      "tax_payable_rate",
    "separate_account_liab",      "separate_account_liab_rate",
    "receivable",      "receivable_rate",
    "payable",      "payable_rate",
    "st_borrowing",      "st_borrowing_rate",
    "federal_funds_purd",      "federal_funds_purd_rate",
    "td_liab",      "td_liab_rate",
    "cash_and_due_from_banks",      "cash_and_due_from_banks_rate",
    "deposits_with_banks",      "deposits_with_banks_rate",
    "federal_funds_sold",      "federal_funds_sold_rate",
    "td_assets",      "td_assets_rate",
    "debt_sec",      "debt_sec_rate",
    "allowance_for_loan_loss",      "allowance_for_loan_loss_rate",
    "net_loan",      "net_loan_rate",
    "mortgage_servicing_rights",      "mortgage_servicing_rights_rate",
    "deposit",      "deposit_rate"
)

balanceDF = pd.DataFrame(columns=BASE_COLUMNS)


def parseUSBalance(code):
    path = "C:/project/stockdata/USBalance/%s.json" % code
    text = readFile(path)
    jsonObj = json.loads(text)
    jsonObjList = jsonObj['data']['list']
    
    balanceDF = pd.DataFrame(columns=COLUMNS)
    jsonData = {'ts_code': code}
    for data in jsonObjList:
        for key, value in data.items():
            if isinstance(value, list):
                jsonData[key] = value[0]
                jsonData[key+'_rate'] = value[1]
            else:
                jsonData[key] = value
        balanceDF = balanceDF.append([jsonData], ignore_index=True)

    balanceDF = balanceDF.drop_duplicates(['ts_code', 'report_date'])
    balanceDF = balanceDF.set_index(['ts_code', 'report_date'])
    balanceDF.to_sql(name='balance', con=usEngine, if_exists='append')


def parseBalanceBase(code):
    FileLogger.info("running on code: %s" % code)
    path = "C:/project/stockdata/USBalance/%s.json" % code
    text = readFile(path)

    if text:
        jsonObj = json.loads(text)
        jsonData = jsonObj['data']
        del jsonData['list']
        jsonData['ts_code'] = code

        global balanceDF
        balanceDF = balanceDF.append([jsonData], ignore_index=True)


def parseBalanceBaseList(codeList):
    for code in codeList:
        parseBalanceBase(code)

    global balanceDF
    balanceDF = balanceDF.drop_duplicates(['ts_code'])
    balanceDF = balanceDF.set_index(['ts_code'])

    balanceDF.to_sql(name='balancebase', con=usEngine, if_exists='append')


def readFile(filePath):
    try:
        fp = open(filePath, 'r')
        content = fp.read()
        return content
    except Exception as ex:
        FileLogger.error("read file error on path: %s" % filePath)
        FileLogger.error(ex)
        return False


if __name__ == "__main__":
    stockdf = pd.read_csv("C:/project/stockdata/USBalance/code.csv")
    stockList = stockdf['code'].to_numpy()

    # parseBalanceBaseList(stockList)

    for code in stockList:
        FileLogger.info("running on code: %s" % code)
        try:
            parseUSBalance(code)

        except Exception as ex:
            FileLogger.error(ex)
            FileLogger.error("write data to Database error on code: %s" % code)
            time.sleep(1)