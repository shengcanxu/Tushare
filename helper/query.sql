create database stock; 
use stock; 

-- create stock daily data table
CREATE TABLE `daily` (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- create income table 
CREATE TABLE `income` (
  `ts_code` varchar(20),
  `end_date` varchar(20),
  `ann_date` varchar(20),
  `f_ann_date` varchar(20),
  `report_type` varchar(10),
  `comp_type` varchar(10),
  `basic_eps` double DEFAULT NULL,
  `diluted_eps` double DEFAULT NULL,
  `total_revenue` double DEFAULT NULL,
  `revenue` double DEFAULT NULL,
  `int_income` double DEFAULT NULL,
  `prem_earned` double DEFAULT NULL,
  `comm_income` double DEFAULT NULL,
  `n_commis_income` double DEFAULT NULL,
  `n_oth_income` double DEFAULT NULL,
  `n_oth_b_income` double DEFAULT NULL,
  `prem_income` double DEFAULT NULL,
  `out_prem` double DEFAULT NULL,
  `une_prem_reser` double DEFAULT NULL,
  `reins_income` double DEFAULT NULL,
  `n_sec_tb_income` double DEFAULT NULL,
  `n_sec_uw_income` double DEFAULT NULL,
  `n_asset_mg_income` double DEFAULT NULL,
  `oth_b_income` double DEFAULT NULL,
  `fv_value_chg_gain` double DEFAULT NULL,
  `invest_income` double DEFAULT NULL,
  `ass_invest_income` double DEFAULT NULL,
  `forex_gain` double DEFAULT NULL,
  `total_cogs` double DEFAULT NULL,
  `oper_cost` double DEFAULT NULL,
  `int_exp` double DEFAULT NULL,
  `comm_exp` double DEFAULT NULL,
  `biz_tax_surchg` double DEFAULT NULL,
  `sell_exp` double DEFAULT NULL,
  `admin_exp` double DEFAULT NULL,
  `fin_exp` double DEFAULT NULL,
  `assets_impair_loss` double DEFAULT NULL,
  `prem_refund` double DEFAULT NULL,
  `compens_payout` double DEFAULT NULL,
  `reser_insur_liab` double DEFAULT NULL,
  `div_payt` double DEFAULT NULL,
  `reins_exp` double DEFAULT NULL,
  `oper_exp` double DEFAULT NULL,
  `compens_payout_refu` double DEFAULT NULL,
  `insur_reser_refu` double DEFAULT NULL,
  `reins_cost_refund` double DEFAULT NULL,
  `other_bus_cost` double DEFAULT NULL,
  `operate_profit` double DEFAULT NULL,
  `non_oper_income` double DEFAULT NULL,
  `non_oper_exp` double DEFAULT NULL,
  `nca_disploss` double DEFAULT NULL,
  `total_profit` double DEFAULT NULL,
  `income_tax` double DEFAULT NULL,
  `n_income` double DEFAULT NULL,
  `n_income_attr_p` double DEFAULT NULL,
  `minority_gain` double DEFAULT NULL,
  `oth_compr_income` double DEFAULT NULL,
  `t_compr_income` double DEFAULT NULL,
  `compr_inc_attr_p` double DEFAULT NULL,
  `compr_inc_attr_m_s` double DEFAULT NULL,
  `ebit` double DEFAULT NULL,
  `ebitda` double DEFAULT NULL,
  `insurance_exp` double DEFAULT NULL,
  `undist_profit` double DEFAULT NULL,
  `distable_profit` double DEFAULT NULL,
  UNIQUE KEY `idx_income_end_date_ts_code_report_type` (`ts_code`,`end_date`,`report_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- create balance table
CREATE TABLE `balance` (
  `ts_code` varchar(20),
  `end_date` varchar(20),
  `ann_date` varchar(20),
  `f_ann_date` varchar(20),
  `report_type` varchar(10),
  `comp_type` varchar(10),
  `total_share` double DEFAULT NULL,
  `cap_rese` double DEFAULT NULL,
  `undistr_porfit` double DEFAULT NULL,
  `surplus_rese` double DEFAULT NULL,
  `special_rese` double DEFAULT NULL,
  `money_cap` double DEFAULT NULL,
  `trad_asset` double DEFAULT NULL,
  `notes_receiv` double DEFAULT NULL,
  `accounts_receiv` double DEFAULT NULL,
  `oth_receiv` double DEFAULT NULL,
  `prepayment` double DEFAULT NULL,
  `div_receiv` double DEFAULT NULL,
  `int_receiv` double DEFAULT NULL,
  `inventories` double DEFAULT NULL,
  `amor_exp` double DEFAULT NULL,
  `nca_within_1y` double DEFAULT NULL,
  `sett_rsrv` double DEFAULT NULL,
  `loanto_oth_bank_fi` double DEFAULT NULL,
  `premium_receiv` double DEFAULT NULL,
  `reinsur_receiv` double DEFAULT NULL,
  `reinsur_res_receiv` double DEFAULT NULL,
  `pur_resale_fa` double DEFAULT NULL,
  `oth_cur_assets` double DEFAULT NULL,
  `total_cur_assets` double DEFAULT NULL,
  `fa_avail_for_sale` double DEFAULT NULL,
  `htm_invest` double DEFAULT NULL,
  `lt_eqt_invest` double DEFAULT NULL,
  `invest_real_estate` double DEFAULT NULL,
  `time_deposits` double DEFAULT NULL,
  `oth_assets` double DEFAULT NULL,
  `lt_rec` double DEFAULT NULL,
  `fix_assets` double DEFAULT NULL,
  `cip` double DEFAULT NULL,
  `const_materials` double DEFAULT NULL,
  `fixed_assets_disp` double DEFAULT NULL,
  `produc_bio_assets` double DEFAULT NULL,
  `oil_and_gas_assets` double DEFAULT NULL,
  `intan_assets` double DEFAULT NULL,
  `r_and_d` double DEFAULT NULL,
  `goodwill` double DEFAULT NULL,
  `lt_amor_exp` double DEFAULT NULL,
  `defer_tax_assets` double DEFAULT NULL,
  `decr_in_disbur` double DEFAULT NULL,
  `oth_nca` double DEFAULT NULL,
  `total_nca` double DEFAULT NULL,
  `cash_reser_cb` double DEFAULT NULL,
  `depos_in_oth_bfi` double DEFAULT NULL,
  `prec_metals` double DEFAULT NULL,
  `deriv_assets` double DEFAULT NULL,
  `rr_reins_une_prem` double DEFAULT NULL,
  `rr_reins_outstd_cla` double DEFAULT NULL,
  `rr_reins_lins_liab` double DEFAULT NULL,
  `rr_reins_lthins_liab` double DEFAULT NULL,
  `refund_depos` double DEFAULT NULL,
  `ph_pledge_loans` double DEFAULT NULL,
  `refund_cap_depos` double DEFAULT NULL,
  `indep_acct_assets` double DEFAULT NULL,
  `client_depos` double DEFAULT NULL,
  `client_prov` double DEFAULT NULL,
  `transac_seat_fee` double DEFAULT NULL,
  `invest_as_receiv` double DEFAULT NULL,
  `total_assets` double DEFAULT NULL,
  `lt_borr` double DEFAULT NULL,
  `st_borr` double DEFAULT NULL,
  `cb_borr` double DEFAULT NULL,
  `depos_ib_deposits` double DEFAULT NULL,
  `loan_oth_bank` double DEFAULT NULL,
  `trading_fl` double DEFAULT NULL,
  `notes_payable` double DEFAULT NULL,
  `acct_payable` double DEFAULT NULL,
  `adv_receipts` double DEFAULT NULL,
  `sold_for_repur_fa` double DEFAULT NULL,
  `comm_payable` double DEFAULT NULL,
  `payroll_payable` double DEFAULT NULL,
  `taxes_payable` double DEFAULT NULL,
  `int_payable` double DEFAULT NULL,
  `div_payable` double DEFAULT NULL,
  `oth_payable` double DEFAULT NULL,
  `acc_exp` double DEFAULT NULL,
  `deferred_inc` double DEFAULT NULL,
  `st_bonds_payable` double DEFAULT NULL,
  `payable_to_reinsurer` double DEFAULT NULL,
  `rsrv_insur_cont` double DEFAULT NULL,
  `acting_trading_sec` double DEFAULT NULL,
  `acting_uw_sec` double DEFAULT NULL,
  `non_cur_liab_due_1y` double DEFAULT NULL,
  `oth_cur_liab` double DEFAULT NULL,
  `total_cur_liab` double DEFAULT NULL,
  `bond_payable` double DEFAULT NULL,
  `lt_payable` double DEFAULT NULL,
  `specific_payables` double DEFAULT NULL,
  `estimated_liab` double DEFAULT NULL,
  `defer_tax_liab` double DEFAULT NULL,
  `defer_inc_non_cur_liab` double DEFAULT NULL,
  `oth_ncl` double DEFAULT NULL,
  `total_ncl` double DEFAULT NULL,
  `depos_oth_bfi` double DEFAULT NULL,
  `deriv_liab` double DEFAULT NULL,
  `depos` double DEFAULT NULL,
  `agency_bus_liab` double DEFAULT NULL,
  `oth_liab` double DEFAULT NULL,
  `prem_receiv_adva` double DEFAULT NULL,
  `depos_received` double DEFAULT NULL,
  `ph_invest` double DEFAULT NULL,
  `reser_une_prem` double DEFAULT NULL,
  `reser_outstd_claims` double DEFAULT NULL,
  `reser_lins_liab` double DEFAULT NULL,
  `reser_lthins_liab` double DEFAULT NULL,
  `indept_acc_liab` double DEFAULT NULL,
  `pledge_borr` double DEFAULT NULL,
  `indem_payable` double DEFAULT NULL,
  `policy_div_payable` double DEFAULT NULL,
  `total_liab` double DEFAULT NULL,
  `treasury_share` double DEFAULT NULL,
  `ordin_risk_reser` double DEFAULT NULL,
  `forex_differ` double DEFAULT NULL,
  `invest_loss_unconf` double DEFAULT NULL,
  `minority_int` double DEFAULT NULL,
  `total_hldr_eqy_exc_min_int` double DEFAULT NULL,
  `total_hldr_eqy_inc_min_int` double DEFAULT NULL,
  `total_liab_hldr_eqy` double DEFAULT NULL,
  `lt_payroll_payable` double DEFAULT NULL,
  `oth_comp_income` double DEFAULT NULL,
  `oth_eqt_tools` double DEFAULT NULL,
  `oth_eqt_tools_p_shr` double DEFAULT NULL,
  `lending_funds` double DEFAULT NULL,
  `acc_receivable` double DEFAULT NULL,
  `st_fin_payable` double DEFAULT NULL,
  `payables` double DEFAULT NULL,
  `hfs_assets` double DEFAULT NULL,
  `hfs_sales` double DEFAULT NULL,
  UNIQUE KEY `idx_balance_end_date_ts_code_report_type` (`ts_code`,`end_date`,`report_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- create cashflow table
CREATE TABLE `cashflow` (
	`ts_code` varchar(20),
	`ann_date` varchar(20),
	`f_ann_date` varchar(20),
	`end_date` varchar(20),
	`comp_type` varchar(10),
	`report_type` varchar(10),
	`net_profit` double DEFAULT NULL,
	`finan_exp` double DEFAULT NULL,
	`c_fr_sale_sg` double DEFAULT NULL,
	`recp_tax_rends` double DEFAULT NULL,
	`n_depos_incr_fi` double DEFAULT NULL,
	`n_incr_loans_cb` double DEFAULT NULL,
	`n_inc_borr_oth_fi` double DEFAULT NULL,
	`prem_fr_orig_contr` double DEFAULT NULL,
	`n_incr_insured_dep` double DEFAULT NULL,
	`n_reinsur_prem` double DEFAULT NULL,
	`n_incr_disp_tfa` double DEFAULT NULL,
	`ifc_cash_incr` double DEFAULT NULL,
	`n_incr_disp_faas` double DEFAULT NULL,
	`n_incr_loans_oth_bank` double DEFAULT NULL,
	`n_cap_incr_repur` double DEFAULT NULL,
	`c_fr_oth_operate_a` double DEFAULT NULL,
	`c_inf_fr_operate_a` double DEFAULT NULL,
	`c_paid_goods_s` double DEFAULT NULL,
	`c_paid_to_for_empl` double DEFAULT NULL,
	`c_paid_for_taxes` double DEFAULT NULL,
	`n_incr_clt_loan_adv` double DEFAULT NULL,
	`n_incr_dep_cbob` double DEFAULT NULL,
	`c_pay_claims_orig_inco` double DEFAULT NULL,
	`pay_handling_chrg` double DEFAULT NULL,
	`pay_comm_insur_plcy` double DEFAULT NULL,
	`oth_cash_pay_oper_act` double DEFAULT NULL,
	`st_cash_out_act` double DEFAULT NULL,
	`n_cashflow_act` double DEFAULT NULL,
	`oth_recp_ral_inv_act` double DEFAULT NULL,
	`c_disp_withdrwl_invest` double DEFAULT NULL,
	`c_recp_return_invest` double DEFAULT NULL,
	`n_recp_disp_fiolta` double DEFAULT NULL,
	`n_recp_disp_sobu` double DEFAULT NULL,
	`stot_inflows_inv_act` double DEFAULT NULL,
	`c_pay_acq_const_fiolta` double DEFAULT NULL,
	`c_paid_invest` double DEFAULT NULL,
	`n_disp_subs_oth_biz` double DEFAULT NULL,
	`oth_pay_ral_inv_act` double DEFAULT NULL,
	`n_incr_pledge_loan` double DEFAULT NULL,
	`stot_out_inv_act` double DEFAULT NULL,
	`n_cashflow_inv_act` double DEFAULT NULL,
	`c_recp_borrow` double DEFAULT NULL,
	`proc_issue_bonds` double DEFAULT NULL,
	`oth_cash_recp_ral_fnc_act` double DEFAULT NULL,
	`stot_cash_in_fnc_act` double DEFAULT NULL,
	`free_cashflow` double DEFAULT NULL,
	`c_prepay_amt_borr` double DEFAULT NULL,
	`c_pay_dist_dpcp_int_exp` double DEFAULT NULL,
	`incl_dvd_profit_paid_sc_ms` double DEFAULT NULL,
	`oth_cashpay_ral_fnc_act` double DEFAULT NULL,
	`stot_cashout_fnc_act` double DEFAULT NULL,
	`n_cash_flows_fnc_act` double DEFAULT NULL,
	`eff_fx_flu_cash` double DEFAULT NULL,
	`n_incr_cash_cash_equ` double DEFAULT NULL,
	`c_cash_equ_beg_period` double DEFAULT NULL,
	`c_cash_equ_end_period` double DEFAULT NULL,
	`c_recp_cap_contrib` double DEFAULT NULL,
	`incl_cash_rec_saims` double DEFAULT NULL,
	`uncon_invest_loss` double DEFAULT NULL,
	`prov_depr_assets` double DEFAULT NULL,
	`depr_fa_coga_dpba` double DEFAULT NULL,
	`amort_intang_assets` double DEFAULT NULL,
	`lt_amort_deferred_exp` double DEFAULT NULL,
	`decr_deferred_exp` double DEFAULT NULL,
	`incr_acc_exp` double DEFAULT NULL,
	`loss_disp_fiolta` double DEFAULT NULL,
	`loss_scr_fa` double DEFAULT NULL,
	`loss_fv_chg` double DEFAULT NULL,
	`invest_loss` double DEFAULT NULL,
	`decr_def_inc_tax_assets` double DEFAULT NULL,
	`incr_def_inc_tax_liab` double DEFAULT NULL,
	`decr_inventories` double DEFAULT NULL,
	`decr_oper_payable` double DEFAULT NULL,
	`incr_oper_payable` double DEFAULT NULL,
	`others` double DEFAULT NULL,
	`im_net_cashflow_oper_act` double DEFAULT NULL,
	`conv_debt_into_cap` double DEFAULT NULL,
	`conv_copbonds_due_within_1y` double DEFAULT NULL,
	`fa_fnc_leases` double DEFAULT NULL,
	`end_bal_cash` double DEFAULT NULL,
	`beg_bal_cash` double DEFAULT NULL,
	`end_bal_cash_equ` double DEFAULT NULL,
	`beg_bal_cash_equ` double DEFAULT NULL,
	`im_n_incr_cash_equ` double DEFAULT NULL,
  UNIQUE KEY `idx_cashflow_end_date_ts_code_report_type` (`ts_code`,`end_date`,`report_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- create finance indicators table
CREATE TABLE `finance` (
  `ts_code` varchar(20),
  `end_date` varchar(20),
  `ann_date` varchar(20),
  `eps` double DEFAULT NULL,
  `dt_eps` double DEFAULT NULL,
  `total_revenue_ps` double DEFAULT NULL,
  `revenue_ps` double DEFAULT NULL,
  `capital_rese_ps` double DEFAULT NULL,
  `surplus_rese_ps` double DEFAULT NULL,
  `undist_profit_ps` double DEFAULT NULL,
  `extra_item` double DEFAULT NULL,
  `profit_dedt` double DEFAULT NULL,
  `gross_margin` double DEFAULT NULL,
  `current_ratio` double DEFAULT NULL,
  `quick_ratio` double DEFAULT NULL,
  `cash_ratio` double DEFAULT NULL,
  `ar_turn` double DEFAULT NULL,
  `ca_turn` double DEFAULT NULL,
  `fa_turn` double DEFAULT NULL,
  `assets_turn` double DEFAULT NULL,
  `op_income` double DEFAULT NULL,
  `ebit` double DEFAULT NULL,
  `ebitda` double DEFAULT NULL,
  `fcff` double DEFAULT NULL,
  `fcfe` double DEFAULT NULL,
  `current_exint` double DEFAULT NULL,
  `noncurrent_exint` double DEFAULT NULL,
  `interestdebt` double DEFAULT NULL,
  `netdebt` double DEFAULT NULL,
  `tangible_asset` double DEFAULT NULL,
  `working_capital` double DEFAULT NULL,
  `networking_capital` double DEFAULT NULL,
  `invest_capital` double DEFAULT NULL,
  `retained_earnings` double DEFAULT NULL,
  `diluted2_eps` double DEFAULT NULL,
  `bps` double DEFAULT NULL,
  `ocfps` double DEFAULT NULL,
  `retainedps` double DEFAULT NULL,
  `cfps` double DEFAULT NULL,
  `ebit_ps` double DEFAULT NULL,
  `fcff_ps` double DEFAULT NULL,
  `fcfe_ps` double DEFAULT NULL,
  `netprofit_margin` double DEFAULT NULL,
  `grossprofit_margin` double DEFAULT NULL,
  `cogs_of_sales` double DEFAULT NULL,
  `expense_of_sales` double DEFAULT NULL,
  `profit_to_gr` double DEFAULT NULL,
  `saleexp_to_gr` double DEFAULT NULL,
  `adminexp_of_gr` double DEFAULT NULL,
  `finaexp_of_gr` double DEFAULT NULL,
  `impai_ttm` double DEFAULT NULL,
  `gc_of_gr` double DEFAULT NULL,
  `op_of_gr` double DEFAULT NULL,
  `ebit_of_gr` double DEFAULT NULL,
  `roe` double DEFAULT NULL,
  `roe_waa` double DEFAULT NULL,
  `roe_dt` double DEFAULT NULL,
  `roa` double DEFAULT NULL,
  `npta` double DEFAULT NULL,
  `roic` double DEFAULT NULL,
  `roe_yearly` double DEFAULT NULL,
  `roa2_yearly` double DEFAULT NULL,
  `debt_to_assets` double DEFAULT NULL,
  `assets_to_eqt` double DEFAULT NULL,
  `dp_assets_to_eqt` double DEFAULT NULL,
  `ca_to_assets` double DEFAULT NULL,
  `nca_to_assets` double DEFAULT NULL,
  `tbassets_to_totalassets` double DEFAULT NULL,
  `int_to_talcap` double DEFAULT NULL,
  `eqt_to_talcapital` double DEFAULT NULL,
  `currentdebt_to_debt` double DEFAULT NULL,
  `longdeb_to_debt` double DEFAULT NULL,
  `ocf_to_shortdebt` double DEFAULT NULL,
  `debt_to_eqt` double DEFAULT NULL,
  `eqt_to_debt` double DEFAULT NULL,
  `eqt_to_interestdebt` double DEFAULT NULL,
  `tangibleasset_to_debt` double DEFAULT NULL,
  `tangasset_to_intdebt` double DEFAULT NULL,
  `tangibleasset_to_netdebt` double DEFAULT NULL,
  `ocf_to_debt` double DEFAULT NULL,
  `turn_days` double DEFAULT NULL,
  `roa_yearly` double DEFAULT NULL,
  `roa_dp` double DEFAULT NULL,
  `fixed_assets` double DEFAULT NULL,
  `profit_to_op` double DEFAULT NULL,
  `q_saleexp_to_gr` double DEFAULT NULL,
  `q_gc_to_gr` double DEFAULT NULL,
  `q_roe` double DEFAULT NULL,
  `q_dt_roe` double DEFAULT NULL,
  `q_npta` double DEFAULT NULL,
  `q_ocf_to_sales` double DEFAULT NULL,
  `basic_eps_yoy` double DEFAULT NULL,
  `dt_eps_yoy` double DEFAULT NULL,
  `cfps_yoy` double DEFAULT NULL,
  `op_yoy` double DEFAULT NULL,
  `ebt_yoy` double DEFAULT NULL,
  `netprofit_yoy` double DEFAULT NULL,
  `dt_netprofit_yoy` double DEFAULT NULL,
  `ocf_yoy` double DEFAULT NULL,
  `roe_yoy` double DEFAULT NULL,
  `bps_yoy` double DEFAULT NULL,
  `assets_yoy` double DEFAULT NULL,
  `eqt_yoy` double DEFAULT NULL,
  `tr_yoy` double DEFAULT NULL,
  `or_yoy` double DEFAULT NULL,
  `q_sales_yoy` double DEFAULT NULL,
  `q_op_qoq` double DEFAULT NULL,
  `equity_yoy` double DEFAULT NULL,
  UNIQUE KEY `idx_cashflow_end_date_ts_code` (`ts_code`,`end_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- create main_business table
CREATE TABLE `mainbusiness` (
  `ts_code` varchar(20),
  `end_date` varchar(20),
  `bz_item` varchar(200),
  `type` varchar(10),
  `bz_sales` double DEFAULT NULL,
  `bz_profit` double DEFAULT NULL,
  `bz_cost` double DEFAULT NULL,
  `curr_type` varchar(10),
  UNIQUE KEY `idx_mainbusiness_end_date_ts_code_type` (`ts_code`,`end_date`, `type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- create adjust factor table
CREATE TABLE `adjustfactor` (
  `ts_code` varchar(20),
  `trade_date` varchar(20),
  `adj_factor` double DEFAULT NULL,
  UNIQUE KEY `idx_adjustfactor_trade_date_ts_code` (`ts_code`,`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `hkstockdata` (
  `ts_code` varchar(20) DEFAULT NULL,
  `name` varchar(20) DEFAULT NULL,
  `fullname` text,
  `enname` text,
  `cn_spell` varchar(20) DEFAULT NULL,
  `market` varchar(20) DEFAULT NULL,
  `list_status` varchar(20) DEFAULT NULL,
  `list_date` varchar(20) DEFAULT NULL,
  `delist_date` varchar(20) DEFAULT NULL,
  `trade_unit` double DEFAULT NULL,
  `isin` varchar(20) DEFAULT NULL,
  `curr_type` varchar(20) DEFAULT NULL,
  UNIQUE KEY `idx_hkdaily_ts_code` (`ts_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `hkdaily` (
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
  unique KEY `ix_hkdaily_ts_code` (`ts_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


----------------------------------------------------------------------------
create database indexdata; 
use indexdata; 

CREATE TABLE `indexdata` (
  `ts_code` varchar(20),
  `name` text,
  `market` text,
  `publisher` text,
  `category` text,
  `base_date` text,
  `base_point` double DEFAULT NULL,
  `list_date` text,
  UNIQUE KEY `idx_indexdata_ts_code` (`ts_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `weight` (
  `index_code` varchar(20),
  `con_code` varchar(20),
  `trade_date` varchar(20),
  `weight` double DEFAULT NULL,
  UNIQUE KEY `idx_weight_index_code_con_code_trade_date` (`index_code`,`con_code`, `trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `dailybasic` (
  `ts_code` varchar(20),
  `trade_date` varchar(20),
  `total_mv` double DEFAULT NULL,
  `float_mv` double DEFAULT NULL,
  `total_share` double DEFAULT NULL,
  `float_share` double DEFAULT NULL,
  `free_share` double DEFAULT NULL,
  `turnover_rate` double DEFAULT NULL,
  `turnover_rate_f` double DEFAULT NULL,
  `pe` double DEFAULT NULL,
  `pe_ttm` double DEFAULT NULL,
  `pb` double DEFAULT NULL, 
  UNIQUE KEY `idx_indexdata_ts_code_trade_date` (`ts_code`, `trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-------------------------------------------------------------------------------
create database usstock; 
use usstock; 

CREATE TABLE `stocklist` (
  `ts_code` varchar(20),
  `name` varchar(40),
  `classify` varchar(20),
  `list_date` varchar(20),
  `delist_date` varchar(20),
  UNIQUE KEY `idx_indexdata_ts_code` (`ts_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

create database fund; 
use fund; 

CREATE TABLE `fundlist` (
  `ts_code` varchar(20),
  `name` text,
  `management` text,
  `custodian` text,
  `fund_type` text,
  `found_date` text,
  `due_date` text,
  `list_date` text,
  `issue_date` text,
  `delist_date` text,
  `issue_amount` double DEFAULT NULL,
  `m_fee` double DEFAULT NULL,
  `c_fee` double DEFAULT NULL,
  `duration_year` double DEFAULT NULL,
  `p_value` double DEFAULT NULL,
  `min_amount` double DEFAULT NULL,
  `exp_return` text,
  `benchmark` text,
  `status` text,
  `invest_type` text,
  `type` text,
  `trustee` text,
  `purc_startdate` text,
  `redm_startdate` text,
  `market` text,
  UNIQUE KEY `ix_fundlist_index` (`ts_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `company` (
  `index` bigint DEFAULT NULL,
  `name` text,
  `shortname` text,
  `province` text,
  `city` text,
  `address` text,
  `phone` text,
  `office` text,
  `website` text,
  `chairman` text,
  `manager` text,
  `reg_capital` double DEFAULT NULL,
  `setup_date` text,
  `end_date` text,
  `employees` double DEFAULT NULL,
  `main_business` text,
  `org_code` text,
  `credit_code` text,
  UNIQUE KEY `ix_company_index` (`index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `manager` (
  `ts_code` varchar(20),
  `name` varchar(20),
  `ann_date` varchar(20),
  `gender` varchar(10),
  `birth_year` varchar(20),
  `edu` varchar(20),
  `nationality` varchar(20),
  `begin_date` varchar(20),
  `end_date` varchar(20),
  `resume` text, 
  UNIQUE KEY `idx_manager_ts_code` (`ts_code`, `name`, `ann_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `share` (
  `ts_code` varchar(20),
  `trade_date` varchar(20),
  `fd_share` double DEFAULT NULL,
  `fund_type` varchar(10),
  `market` varchar(10),
  UNIQUE KEY `idx_share_ts_code_trade_date` (`ts_code`, `trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `nav` (
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
  UNIQUE KEY `idx_nav_ts_code_end_date_adj_nav` (`ts_code`, `end_date`, `adj_nav`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `dividend` (
  `ts_code` varchar(20),
  `ann_date` varchar(20),
  `imp_anndate` varchar(20),
  `base_date` varchar(20),
  `div_proc` varchar(20),
  `record_date` varchar(20),
  `ex_date` varchar(20),
  `pay_date` varchar(20),
  `earpay_date` varchar(20),
  `net_ex_date` varchar(20),
  `div_cash` double DEFAULT NULL,
  `base_unit` double DEFAULT NULL,
  `ear_distr` double DEFAULT NULL,
  `ear_amount` double DEFAULT NULL,
  `account_date` varchar(20),
  `base_year` varchar(20),
  UNIQUE KEY `idx_dividend_ts_code_ann_date` (`ts_code`, `ann_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `portfolio` (
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


CREATE TABLE `daily` (
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

CREATE TABLE `adjust` (
  `ts_code` varchar(20),
  `trade_date` varchar(20),
  `adj_factor` double DEFAULT NULL,
  KEY `idx_adjust_ts_code_trade_date` (`ts_code`, `trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



create database macro; 
use macro; 

CREATE TABLE `shibor` (
  `date` varchar(20),
  `on` double DEFAULT NULL,
  `1w` double DEFAULT NULL,
  `2w` double DEFAULT NULL,
  `1m` double DEFAULT NULL,
  `3m` double DEFAULT NULL,
  `6m` double DEFAULT NULL,
  `9m` double DEFAULT NULL,
  `1y` double DEFAULT NULL,
  unique KEY `idx_shibor_date` (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `shiborquote` (
  `date` varchar(20),
  `bank` varchar(40),
  `on_b` double DEFAULT NULL,
  `on_a` double DEFAULT NULL,
  `1w_b` double DEFAULT NULL,
  `1w_a` double DEFAULT NULL,
  `2w_b` double DEFAULT NULL,
  `2w_a` double DEFAULT NULL,
  `1m_b` double DEFAULT NULL,
  `1m_a` double DEFAULT NULL,
  `3m_b` double DEFAULT NULL,
  `3m_a` double DEFAULT NULL,
  `6m_b` double DEFAULT NULL,
  `6m_a` double DEFAULT NULL,
  `9m_b` double DEFAULT NULL,
  `9m_a` double DEFAULT NULL,
  `1y_b` double DEFAULT NULL,
  `1y_a` double DEFAULT NULL,
  unique KEY `idx_shiborquote_date` (`date`, `bank`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `shiborlpr` (
  `date` varchar(20),
  `1y` double DEFAULT NULL,
  unique KEY `idx_shiborlpr_date` (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `libor` (
  `date` varchar(20),
  `curr_type` varchar(20),
  `on` double DEFAULT NULL,
  `1w` double DEFAULT NULL,
  `1m` double DEFAULT NULL,
  `2m` double DEFAULT NULL,
  `3m` double DEFAULT NULL,
  `6m` double DEFAULT NULL,
  `12m` double DEFAULT NULL,
  KEY `idx_libor_date_curr_type` (`date`, `curr_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `hibor` (
  `date` varchar(20),
  `on` double DEFAULT NULL,
  `1w` double DEFAULT NULL,
  `2w` double DEFAULT NULL,
  `1m` double DEFAULT NULL,
  `2m` double DEFAULT NULL,
  `3m` double DEFAULT NULL,
  `6m` double DEFAULT NULL,
  `12m` double DEFAULT NULL,
  KEY `idx_hibor_date` (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `wzindex` (
  `date` varchar(20),
  `comp_rate` double DEFAULT NULL,
  `center_rate` double DEFAULT NULL,
  `micro_rate` double DEFAULT NULL,
  `cm_rate` double DEFAULT NULL,
  `sdb_rate` double DEFAULT NULL,
  `om_rate` double DEFAULT NULL,
  `aa_rate` double DEFAULT NULL,
  `m1_rate` double DEFAULT NULL,
  `m3_rate` double DEFAULT NULL,
  `m6_rate` double DEFAULT NULL,
  `m12_rate` double DEFAULT NULL,
  `long_rate` double DEFAULT NULL,
  KEY `idx_wzindex_date` (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `gzindex` (
  `date` varchar(20),
  `d10_rate` double DEFAULT NULL,
  `m1_rate` double DEFAULT NULL,
  `m3_rate` double DEFAULT NULL,
  `m6_rate` double DEFAULT NULL,
  `m12_rate` double DEFAULT NULL,
  `long_rate` double DEFAULT NULL,
  KEY `idx_gzindex_date` (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `exchangedata` (
  `ts_code` varchar(20),
  `name` varchar(40),
  `classify` varchar(20),
  `exchange` varchar(20),
  `min_unit` double DEFAULT NULL,
  `max_unit` double DEFAULT NULL,
  `pip` double DEFAULT NULL,
  `pip_cost` double DEFAULT NULL,
  `traget_spread` double DEFAULT NULL,
  `min_stop_distance` double DEFAULT NULL,
  `trading_hours` varchar(100),
  `break_time` varchar(100),
  UNIQUE KEY `idx_exchangedata_ts_code` (`ts_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `fxdaily` (
  `ts_code` varchar(20),
  `trade_date` varchar(20),
  `bid_open` double DEFAULT NULL,
  `bid_close` double DEFAULT NULL,
  `bid_high` double DEFAULT NULL,
  `bid_low` double DEFAULT NULL,
  `ask_open` double DEFAULT NULL,
  `ask_close` double DEFAULT NULL,
  `ask_high` double DEFAULT NULL,
  `ask_low` double DEFAULT NULL,
  `tick_qty` bigint DEFAULT NULL,
  unique KEY `ix_fxdaily_ts_code_trade_date` (`ts_code`, `trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


create database usstock; 
use usstock; 

CREATE TABLE `daily` (
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









