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
    "total_revenue",      "total_revenue_rate",
    "income_from_co_before_it",      "income_from_co_before_it_rate",
    "net_income_atcss",      "net_income_atcss_rate",
    "income_from_co_before_tax_si",      "income_from_co_before_tax_si_rate",
    "income_tax",      "income_tax_rate",
    "income_from_co",      "income_from_co_rate",
    "net_income",      "net_income_rate",
    "preferred_dividend",      "preferred_dividend_rate",
    "net_income_atms_interest",      "net_income_atms_interest_rate",
    "total_compre_income_atms",      "total_compre_income_atms_rate",
    "total_compre_income_atcss",      "total_compre_income_atcss_rate",
    "total_basic_earning_common_ps",      "total_basic_earning_common_ps_rate",
    "total_dlt_earnings_common_ps",      "total_dlt_earnings_common_ps_rate",
    "total_compre_income",      "total_compre_income_rate",
    "total_net_income_atcss",      "total_net_income_atcss_rate",
    "revenue",      "revenue_rate",
    "othr_revenues",      "othr_revenues_rate",
    "sales_cost",      "sales_cost_rate",
    "gross_profit",      "gross_profit_rate",
    "marketing_selling_etc",      "marketing_selling_etc_rate",
    "rad_expenses",      "rad_expenses_rate",
    "net_interest_expense",      "net_interest_expense_rate",
    "interest_income",      "interest_income_rate",
    "interest_expense",      "interest_expense_rate",
    "total_operate_expenses_si",      "total_operate_expenses_si_rate",
    "total_operate_expenses",      "total_operate_expenses_rate",
    "operating_income",      "operating_income_rate",
    "share_of_earnings_of_affiliate",      "share_of_earnings_of_affiliate_rate",
    "preminum",      "preminum_rate",
    "net_invest_income",      "net_invest_income_rate",
    "net_invest_gains",      "net_invest_gains_rate",
    "service_commi_and_fees",      "service_commi_and_fees_rate",
    "total_revenue_special_subject",      "total_revenue_special_subject_rate",
    "loss_and_loss_adjust_cost",      "loss_and_loss_adjust_cost_rate",
    "policyholder_benefits_etc",      "policyholder_benefits_etc_rate",
    "total_benefit_etc_si",      "total_benefit_etc_si_rate",
    "total_benefit_claim_and_cost",      "total_benefit_claim_and_cost_rate",
    "operating_expenses",      "operating_expenses_rate",
    "depreciation_and_amortization",      "depreciation_and_amortization_rate",
    "total_expense_special_subject",      "total_expense_special_subject_rate",
    "total_expense",      "total_expense_rate",
    "tech_communication_and_equip",      "tech_communication_and_equip_rate",
    "net_occupancy",      "net_occupancy_rate",
    "total_non_interest_cost_si",      "total_non_interest_cost_si_rate",
    "total_non_interest_expense",      "total_non_interest_expense_rate",
    "invest_banking",      "invest_banking_rate",
    "asset_manage_and_sec_services",      "asset_manage_and_sec_services_rate",
    "compen_and_benefit",      "compen_and_benefit_rate",
    "advertising_and_marketing",      "advertising_and_marketing_rate",
    "loan_and_lease",      "loan_and_lease_rate",
    "sec",      "sec_rate",
    "td_assets",      "td_assets_rate",
    "total_interest_income_si",      "total_interest_income_si_rate",
    "total_interest_income",      "total_interest_income_rate",
    "deposit",      "deposit_rate",
    "st_borrowing",      "st_borrowing_rate",
    "lt_debt",      "lt_debt_rate",
    "td_liab",      "td_liab_rate",
    "total_interest_expense_si",      "total_interest_expense_si_rate",
    "total_interest_expense",      "total_interest_expense_rate",
    "net_interest_income",      "net_interest_income_rate",
    "commission",      "commission_rate",
    "cerdit_card_income",      "cerdit_card_income_rate",
    "service_charge",      "service_charge_rate",
    "sec_gain",      "sec_gain_rate",
    "insurance_income",      "insurance_income_rate",
    "td_activity",      "td_activity_rate",
    "total_non_interest_income_si",      "total_non_interest_income_si_rate",
    "total_non_interest_income",      "total_non_interest_income_rate",
    "salaries_and_employee_benefits",      "salaries_and_employee_benefits_rate",
    "provision_for_credit_loss",      "provision_for_credit_loss_rate"
)

incomeBaseDF = pd.DataFrame(columns=BASE_COLUMNS)


def parseUSIncome(code):
    path = "C:/project/stockdata/USIncome/%s.json" % code
    text = readFile(path)
    jsonObj = json.loads(text)
    jsonObjList = jsonObj['data']['list']
    
    incomeDF = pd.DataFrame(columns=COLUMNS)
    jsonData = {'ts_code': code}
    for data in jsonObjList: 
        for key, value in data.items():
            if isinstance(value, list):
                jsonData[key] = value[0]
                jsonData[key+'_rate'] = value[1]
            else:
                jsonData[key] = value
        incomeDF = incomeDF.append([jsonData], ignore_index=True)

    incomeDF = incomeDF.drop_duplicates(['ts_code', 'report_date'])
    incomeDF = incomeDF.set_index(['ts_code', 'report_date'])
    incomeDF.to_sql(name='income', con=usEngine, if_exists='append')


def parseIncomeBase(code):
    FileLogger.info("running on code: %s" % code)
    path = "C:/project/stockdata/USIncome/%s.json" % code
    text = readFile(path)

    if text:
        jsonObj = json.loads(text)
        jsonData = jsonObj['data']
        del jsonData['list']
        jsonData['ts_code'] = code

        global incomeBaseDF
        incomeBaseDF = incomeBaseDF.append([jsonData], ignore_index=True)


def parseIncomeBaseList(codeList):
    for code in codeList:
        parseIncomeBase(code)

    global incomeBaseDF
    incomeBaseDF = incomeBaseDF.drop_duplicates(['ts_code'])
    incomeBaseDF = incomeBaseDF.set_index(['ts_code'])

    incomeBaseDF.to_sql(name='incomebase', con=usEngine, if_exists='append')


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
     
    stockdf = pd.read_csv("C:/project/stockdata/USIncome/code.csv")
    stockList = stockdf['code'].to_numpy()

    # parseIncomeBaseList(stockList)

    for code in stockList:
        FileLogger.info("running on code: %s" % code)
        try:
            parseUSIncome(code)

        except Exception as ex:
            FileLogger.error(ex)
            FileLogger.error("write data to Database error on code: %s" % code)
            time.sleep(1)