########################################
## WATCH LIST RISK METRIC
########################################
#
# FUNCTIONS FILE
#
# Author: Marcos Jaen Cortes, Regulatory Strategy and Research
# Created February 2023
#
# This script contains all my custom functions that may be used across projects
#
########################################

pacman::p_load(tidyverse, cansim, smooth, mFilter, forcats, stringr, janitor, readxl, xts, data.table,
               tibbletime, zoo, lubridate, Rblpapi, gridExtra, seasonal, rvest, httr, curl, tinytest, 
               readxl, httr, purrr, rio, timeDate, bit, dplyr, PerformanceAnalytics, DescTools) 

start.date.cansim <- "2000-01-01"

## FUNCTIONS ----------------------------

## Same Link Function (Used when different data, has same link source, example: MRTS Data) 

# SAME_LINK <- function(MTRS.url){
#   MTRS.url <- read_html(MTRS.url) %>% html_nodes("a") %>% html_attr("href") %>% as.data.frame() %>% rename(col = ".")
#   MTRS.url <- MTRS.url %>% filter(grepl("MTRS",col)) %>% distinct()
#   MTRS.url <- paste0("https://www.iiroc.ca",MTRS.url$col)
#   return(MTRS.url)
# }



#IIROC MARKETPLACE DATA (Equity Trading) =================================
#webpage: https://www.iiroc.ca/sections/markets/reports-statistics-and-other-information/reports-market-share-marketplace
#Historical data from 2015 to present

# Function loads and formats the input excel file from IIROC and outputs a df with the desired format and information
IIROC_df_MarketplaceStats <- function(url_iiroc_marketplace){
  
  #url_iiroc_marketplace <- "https://www.iiroc.ca/sections/markets/reports-statistics-and-other-information/reports-market-share-marketplace"
  link <- session(url_iiroc_marketplace) %>% html_nodes("a") %>% html_attr("href") %>% as.data.frame() %>% rename(col = ".")
  link <- link %>% filter(grepl("media/",col)) %>% slice(2)
  url_iiroc_marketplace <- paste0(link$col) %>% as.character()
  GET(url_iiroc_marketplace, write_disk(tf <- tempfile(fileext = ".xlsx")))
  data <- import_list(tf, c("Value Traded", "Volume Traded" ))
  options(digits=20) #precision of as.numeric(x)
  for (i in 1:length(data)){
    data[[names(data)[i]]] <- janitor::row_to_names(data[[names(data)[i]]], 1) #deletes row 1 and makes row 2 the header for all files
    data[[names(data)[i]]][is.na(data[[names(data)[i]]])] <- 0 #convert NA's to 0's
    data[[names(data)[i]]] <- data[[names(data)[i]]][,1:3]
    data[[names(data)[i]]][,3] <- sapply(data[[names(data)[i]]][,3], as.numeric) #making str numeric
    data[[names(data)[i]]]$Month <- ceiling_date(ym((data[[i]]$Month)), "month") - days(1) #format dates to desired format
  }
  
  for (i in 1:length(data)){
    data[[names(data)[i]]] <- data[[names(data)[i]]][data[[names(data)[i]]]$"Trade Type and Listing Market" == "All Trade All Listing Total", ] #Filter through rows with unwanted trade type and listing market
    data[[names(data)[i]]] <- data[[names(data)[i]]][-2]
  }
  
  IIROC_Equity_df <- left_join(data[["Value Traded"]],data[["Volume Traded"]], by = "Month")
  expect_equal(nrow(data[["Value Traded"]]), nrow(data[["Volume Traded"]]))
  if (TRUE){
    IIROC_Equity_df <- IIROC_Equity_df %>% rename("date" = 1,
                                                  "equity_trading_total_value" = 2,
                                                  "equity_trading_total_number" = 3)  %>% 
      mutate(date = ceiling_date(date,"quarters")-1)  %>% 
      group_by(date) %>%
      summarize(equity_trading_total_number = sum(equity_trading_total_number), equity_trading_total_value = sum(equity_trading_total_value))
    return(IIROC_Equity_df)
  }
  return('#####ERROR WITH IIROC EQUITY MARKETSHARE DF#########')
}

IIROC_DF_MRTS <- function(iiroc_mrts_file) {
  #iiroc_mrts_file <- 'T:\\SRWG\\RSR\\SRWL_Targets\\Input Files\\IIROC_DF_MTRS2WebStats.xlsx'
  bond_repo <- read_xlsx(iiroc_mrts_file, sheet = "bond_repo")
  bond_repo[,2:5] <- sapply(bond_repo[,2:5], as.numeric) 
  bond_repo[1] <-  as.yearqtr("2018 Q1") + seq(0, length = NROW(bond_repo)) / 4
  
  corp_bond <- read_xlsx(iiroc_mrts_file, sheet = "corp_bond")
  corp_bond[,2:4] <- sapply(corp_bond[,2:4], as.numeric) 
  corp_bond[1] <-  as.yearqtr("2018 Q1") + seq(0, length = NROW(corp_bond)) / 4
  
  
  IIROC_DF_MTRS <- left_join(corp_bond, bond_repo, by='date') %>%
    mutate(date = floor_date(yq((date)), "month") - days(1))
  
  return(IIROC_DF_MTRS)
}
##---- Export CURRENT IIROC_MRTS2WEBSTATS 
# IIROC_DF_MTRS2WebStats_part1 <- function(a="dummy"){
#   MTRS.url <- SAME_LINK("https://www.iiroc.ca/sections/markets/reports-statistics-and-other-information/bond-and-money-market-secondary-trading")
#   
#   GET(MTRS.url, write_disk(tf <- tempfile(fileext = ".xlsx")))
#   data <- read_excel(tf, sheet = "BOND", col_names = FALSE,range = cell_cols("A:U"))
#   data <- data[7:nrow(data),c(1,18,19,20,21)] #references dates and corp bond data in sheet
#   colnames(data) <- c("date","corporate_bond_trading_under3" ,"corporate_bond_trading_3to10","corporate_bond_trading_over10", "corporate_bond_trading_total")
#   ## Organize and make quarterly (uses "tidyverse")
#   options(digits=20) #precision of as.numeric(x)
#   data <- na.omit(data) 
#   data[,2:5] <- sapply(data[,2:5], as.numeric)
#   data['corporate_bond_trading_under10'] = data$corporate_bond_trading_under3 + data$corporate_bond_trading_3to10
#   df_corporate_bonds <- data %>%
#     filter(grepl("Q",date),!grepl("TOTAL",date)) %>% 
#     select(c(date, corporate_bond_trading_under10, corporate_bond_trading_over10, corporate_bond_trading_total))
#   return(df_corporate_bonds)
# }
# 
# IIROC_DF_MTRS2WebStats_part2 <- function(b="dummy"){
#   MTRS.url <- SAME_LINK("https://www.iiroc.ca/sections/markets/reports-statistics-and-other-information/bond-and-money-market-secondary-trading")
#   
#   GET(MTRS.url, write_disk(tf <- tempfile(fileext = ".xlsx")))
#   data <- read_excel(tf,sheet = "BOND_REPO",col_names = FALSE,range = cell_cols("A:U")) 
#   data <- data[7:nrow(data),c(1,3,18,19,20,21)] 
#   colnames(data) <- c("date", "all_bond_repo", "corporate_bond_repo_under3","corporate_bond_repo_3to10","corporate_bond_repo_over10", "corporate_bond_repo_total")
#   data <- na.omit(data)
#   options(digits=20) #precision of as.numeric(x)
#   data[,2:6] <- sapply(data[,2:6], as.numeric)
#   data['corporate_bond_repo_under10'] = data$corporate_bond_repo_under3 + data$corporate_bond_repo_3to10
#   df_corporate_bonds_repo <- data %>%
#     filter(grepl("Q",date),!grepl("TOTAL",date)) %>% 
#     select(c(date, corporate_bond_repo_under10, corporate_bond_repo_over10,corporate_bond_repo_total,all_bond_repo))
#   
#   return(df_corporate_bonds_repo)
# }

TSX_MiG <- function(tsx_mig_file){
  #tsx_mig_file <- 'T:\\SRWG\\RSR\\SRWL_Targets\\Input Files\\TSX_MIG.xlsx'#update manually
  df_TSX_MiG <- read_xlsx(tsx_mig_file, sheet = 'Sheet2') %>% 
    na.omit(df_TSX_MiG) %>% 
    mutate(date = ceiling_date(date,"quarters")-1) %>%
    group_by(date) %>%
    summarize(equity_issuance_gross = sum(issuance_mthly))
  return(df_TSX_MiG)
  
}

CREA_HOUSING <- function(crea_housing_file) {
  #crea_housing_file <- 'T:\\SRWG\\RSR\\SRWL_Targets\\Input Files\\Seasonally Adjusted.xlsx' #update manually the file
  housing_data <- import_list(crea_housing_file, which = c("AGGREGATE", "GREATER_VANCOUVER", "CALGARY",
                                                           "OTTAWA", "GREATER_TORONTO", "MONTREAL_CMA"))
  
  library(purrr)
  options(digits=20) #precision of as.numeric(x)
  for (i in 1:length(housing_data)){
    housing_data[[names(housing_data)[i]]] <- housing_data[[names(housing_data)[i]]][,c('Date', 'Composite_Benchmark_SA')]
    housing_data[[names(housing_data)[i]]][is.na(housing_data[[names(housing_data)[i]]])] <- 0 #convert NA's to 0's
    housing_data[[names(housing_data)[i]]][,2] <- sapply(housing_data[[names(housing_data)[i]]][,2], as.numeric) #making str numeric
  }
  housing_data_df <- purrr::reduce(list(housing_data[["AGGREGATE"]],
                                        housing_data[["GREATER_VANCOUVER"]], 
                                        housing_data[["CALGARY"]],housing_data[["OTTAWA"]],
                                        housing_data[["GREATER_TORONTO"]], housing_data[["MONTREAL_CMA"]]), dplyr::left_join, by = 'Date')  %>%
    rename( "house_price_canada" = "Composite_Benchmark_SA.x",
            "house_price_gva" = "Composite_Benchmark_SA.y",
            "house_price_calgary" = "Composite_Benchmark_SA.x.x",
            "house_price_ottawa" = "Composite_Benchmark_SA.y.y", 
            "house_price_gta" = "Composite_Benchmark_SA.x.x.x", 
            "house_price_gma" = "Composite_Benchmark_SA.y.y.y") %>%
    mutate(date = ceiling_date(Date,"quarters")-1) %>%
    group_by(date) %>%
    summarize(house_price_canada = last(house_price_canada), 
              house_price_gva = last(house_price_gva),
              house_price_calgary = last(house_price_calgary), 
              house_price_ottawa = last(house_price_ottawa), 
              house_price_gta = last(house_price_gta), 
              house_price_gma = last(house_price_gma))
  return(housing_data_df)
}
##-------For NOT SEASONALLY ADJUSTED 
# CREA_HOUSING <- function(file) {
#   #filename <- 'T:\\SRWG\\RSR\\SRWL_Targets\\Input Files\\Not Seasonally Adjusted.xlsx'#update manually the file
#   housing_data <- import_list(file, which = c("Aggregate", "Greater_Vancouver", "Calgary",
#                                                   "Ottawa", "Greater_Toronto", "Montreal_CMA"))
#   options(digits=20) #precision of as.numeric(x)
#   for (i in 1:length(housing_data)){
#     housing_data[[names(housing_data)[i]]] <- housing_data[[names(housing_data)[i]]][,c('Date', 'Composite_Benchmark')]
#     housing_data[[names(housing_data)[i]]][is.na(housing_data[[names(housing_data)[i]]])] <- 0 #convert NA's to 0's
#     housing_data[[names(housing_data)[i]]][,2] <- sapply(housing_data[[names(housing_data)[i]]][,2], as.numeric) #making str numeric
#   }
#   housing_data_df <- purrr::reduce(list(housing_data[["Aggregate"]],
#                                         housing_data[["Greater_Vancouver"]], 
#                                         housing_data[["Calgary"]],housing_data[["Ottawa"]],
#                                         housing_data[["Greater_Toronto"]], housing_data[["Montreal_CMA"]]), dplyr::left_join, by = 'Date')  %>%
#     rename("house_price_canada" = "Composite_Benchmark.x","house_price_gva" = "Composite_Benchmark.y",
#            "house_price_calgary" = "Composite_Benchmark.x.x",
#            "house_price_ottawa" = "Composite_Benchmark.y.y", 
#            "house_price_gta" = "Composite_Benchmark.x.x.x", 
#            "house_price_gma" = "Composite_Benchmark.y.y.y")  %>%
#     mutate(date = as.yearqtr(as.Date(Date, "%m/%d/%Y"))) %>%
#     group_by(date) %>%
#     summarize(house_price_canada = last(house_price_canada), 
#               house_price_gva = last(house_price_gva),
#               house_price_calgary = last(house_price_calgary), 
#               house_price_ottawa = last(house_price_ottawa), 
#               house_price_gta = last(house_price_gta), 
#               house_price_gma = last(house_price_gma))
#   return(housing_data_df)
# }

# Compute Debt-Service Ratio for Private Non-financial corporates ========================================================================
debt_service_ratio_nfc <- function(nfc_dsr_file) {
  #nfc_dsr_file <- 'T:\\SRWG\\RSR\\Script New\\Input files\\CapIQ_financial_statements_data.rds'
  nfc_dsr <- readRDS(nfc_dsr_file) %>% 
    mutate(date = ceiling_date(date,"quarters")-1) %>% 
    filter(measure %in% c("ebitda","int_exp","current_debt")) %>% 
    group_by(date, measure) %>% 
    summarise(value = last(value,na.rm = TRUE)) %>% 
    ungroup() %>% 
    pivot_wider(names_from = measure, values_from = value) %>% 
    mutate(debt_service_ratio_nfc = 100*(current_debt-int_exp)/ebitda) %>% 
    gather("metric",VALUE, 2:5) %>% 
    filter(metric == "debt_service_ratio_nfc")
  return(nfc_dsr)
}

PMIC_SIZE <- function(PMIC_SIZE_file) { 
  # Import PMIC and Exempt market data ==============================================================
  #PMIC_SIZE_file <- 'T:\\SRWG\\RSR\\SRWL_Targets\\Input Files\\PMIC_Size.xlsx'
  PMIC.data <- read_excel(PMIC_SIZE_file, sheet = "PMIC_Size", col_names = c('date',"PMIC size, Atrium",	
                                                                             "PMIC size, Firm Capital",	
                                                                             "PMIC size, MCAN",	
                                                                             "PMIC size, PrimeWest",
                                                                             "PMIC size, Terra Firma",	
                                                                             "PMIC size, Timbercreek", 
                                                                             "PMIC size, Trez Capital",	
                                                                             "PMIC size, Builders Capital"),
                          range = cell_cols(4:12), col_types = c("date","numeric", 
                                                                 "numeric", "numeric",
                                                                 "numeric", "numeric", 
                                                                 "numeric",
                                                                 "numeric", "numeric")) %>% 
    slice(3:n())
  
  PMIC_Size <- PMIC.data %>%
    mutate(date = ceiling_date(date,"quarters")-1,
           pmic_size_total = replace_na(`PMIC size, Atrium`,0) +
             replace_na(`PMIC size, Firm Capital`,0) +
             replace_na(`PMIC size, MCAN`, 0) +
             replace_na(`PMIC size, PrimeWest`,0) +
             replace_na(`PMIC size, Terra Firma`,0) +
             replace_na(`PMIC size, Timbercreek`,0)) %>%
    gather("metric", VALUE, 2:10) %>%
    arrange(date) %>%
    filter(metric %in% c("pmic_size_total"))
  PMIC_Size$date <- as.Date(PMIC_Size$date)
  return(PMIC_Size)
  
}

BOC_DATA <- function(boc_prices_url) {
  #boc_prices_url <- 'https://www.bankofcanada.ca/rates/price-indexes/bcpi/'
  link <- read_html(boc_prices_url) %>% html_nodes("a") %>% html_attr("href") %>% as.data.frame() %>% rename(col = ".")
  link <- link %>% filter(grepl("BCPI_MONTHLY/csv",col))
  boc_prices_url <- paste0(link$col)
  GET(boc_prices_url, write_disk(tf <- tempfile(fileext = ".csv")))
  data.bcpi_components <- read.csv(tf, skip = 20) %>%  
    mutate(date = as.Date(date)) %>%
    filter(month(date) %in% c(3,6,9,12)) %>%
    mutate(date = ceiling_date(date,"quarters")-1) %>% 
    group_by(date) %>% 
    summarize_all(last) %>% 
    ungroup() %>% 
    gather("metric","VALUE",2:8) %>% 
    mutate(metric = recode(metric, 
                           "M.BCPI" = "commodities_price_index_all",
                           "M.BCNE" = "commodities_price_index_non-energy",
                           "M.ENER" = "commodities_price_index_energy",
                           "M.MTLS" = "commodities_price_index_metals",
                           "M.FOPR" = "commodities_price_index_forestry",
                           "M.AGRI" = "commodities_price_index_agriculture",
                           "M.FISH" = "commodities_price_index_fisheries"))
  
  # Only a few compnents will be taken into the watchlist indicators ========================
  db.boc<- data.bcpi_components %>% 
    filter(metric %in% c("commodities_price_index_all",
                         "commodities_price_index_energy",
                         "commodities_price_index_non-energy"))
  
  db.boc <- db.boc %>% mutate(VALUE = as.numeric(VALUE)) %>% 
    pivot_wider(names_from = metric, values_from = VALUE)
  return(db.boc) #date format is different 
  
}

#STATCAN FUNCTIONS, ALL ARE IN MILLIONS BY DENOMINATION ==========================================
STATCAN_BONDS_ISSUED <- function(a="dummy") {
  # Net Corporate Bond Issuance =================================
  #All non-government corporate issuance in all markets and currencies
  temp <- tempfile()
  download.file("https://www150.statcan.gc.ca/n1/tbl/csv/36100602-eng.zip",temp)
  cansim_bonds_issued <- read.csv(unz(temp, "36100602.csv"), encoding="UTF-8", stringsAsFactors = FALSE)
  unlink(temp)
  
  cansim_bonds_issued$X.U.FEFF.REF_DATE <- ceiling_date(ym((cansim_bonds_issued$X.U.FEFF.REF_DATE)), "month") - days(1)
  
  Bonds_Issued <- cansim_bonds_issued %>% 
    filter(`Sector` %in% c("Non-financial corporations","Financial corporations"),
           `Maturity` %in% c("Long-term at original maturity"),
           `Interest.rate.type` %in% c("All types of interest rate"),
           `Currency` %in% c("All currencies"),
           `Market.of.issuance` %in% c("All markets of issuance")) %>% 
    mutate(date = as.yearqtr(as.Date(X.U.FEFF.REF_DATE, "%m/%d/%Y")), "metric" = "corporate_bond_issuance_net") %>%
    select(date, metric, VALUE) %>% 
    group_by(date,metric) %>% 
    summarize(VALUE = sum(VALUE))
  
  Bonds_Issued$date <- floor_date(yq((Bonds_Issued$date)), "month") - days(1)
  return(Bonds_Issued)
}

STATCAN_GDP <- function(b="dummy") {
  # Get GDP series (output: GDP) =================================
  temp <- tempfile()
  download.file("https://www150.statcan.gc.ca/n1/tbl/csv/36100104-eng.zip",temp)
  data <- read.csv(unz(temp, "36100104.csv"), encoding="UTF-8", stringsAsFactors = FALSE)
  unlink(temp)
  data$X.U.FEFF.REF_DATE <- ceiling_date(ym((data$X.U.FEFF.REF_DATE)), "month") - days(1)
  
  gdp.n <- data %>% 
    filter(as.Date(X.U.FEFF.REF_DATE) > as.Date("1989-12-31"),
           Estimates %in% c("Gross domestic product at market prices"),
           Prices %in% c("Current prices"),
           `Seasonal.adjustment` == "Seasonally adjusted at annual rates") %>%
    select(X.U.FEFF.REF_DATE, metric = Estimates, VALUE) %>% 
    mutate(date = as.yearqtr(as.Date(X.U.FEFF.REF_DATE, "%m/%d/%Y")), 
           metric = "gdp_nominal")
  
  gdp.n <- gdp.n %>%
    select(date, metric, VALUE)
  gdp.comp <- data %>% # For economic monitoring
    filter(Estimates %in% c("Gross domestic product at market prices",
                            "Final consumption expenditure",
                            "General governments final consumption expenditure", #note: included in Final consumption with HH's
                            "Gross fixed capital formation", # aka "investment"
                            "Investment in inventories",
                            "Exports of goods and services",
                            "Less: imports of goods and services",
                            "Statistical discrepancy"),
           Prices %in% c("Contributions to percent change, annualized"),
           `Seasonal.adjustment` == "Seasonally adjusted at annual rates") %>% 
    select(X.U.FEFF.REF_DATE, metric = Estimates, VALUE) 
  # Formula: FCE (includes gov) + Inv + Inv Inventories + Ex - Im + discrepancy
  gdp.n$date <- floor_date(yq((gdp.n$date)), "month") - days(1)
  
  return(gdp.n)
}

STATCAN_DTE <- function(c="dummy") {
  # Pull Debt-to-Equity ratio for non-financial corporates (output: DTE)  =================================
  temp <- tempfile()
  download.file("https://www150.statcan.gc.ca/n1/tbl/csv/38100236-eng.zip",temp)
  data <- read.csv(unz(temp, "38100236.csv"), encoding="UTF-8", stringsAsFactors = FALSE)
  unlink(temp)
  data$X.U.FEFF.REF_DATE <- ceiling_date(ym((data$X.U.FEFF.REF_DATE)), "month") - days(1)
  
  dte <- data %>% 
    filter(grepl("debt to equity", Categories), grepl("market value", Categories)) %>%
    select(X.U.FEFF.REF_DATE, metric = Categories, VALUE) %>% 
    mutate(date = as.yearqtr(as.Date(X.U.FEFF.REF_DATE, "%m/%d/%Y")), 
           metric = "debt_to_equity_nfc") 
  dte <- dte %>% 
    select(date, metric, VALUE)
  dte$date <- floor_date(yq((dte$date)), "month") - days(1)
  return(dte)
}

STATCAN_HHBurden <- function(d='dummy') {
  # Pull Household debt-service and disposable income  =================================
  
  temp <- tempfile()
  download.file("https://www150.statcan.gc.ca/n1/tbl/csv/11100065-eng.zip",temp)
  data <- read.csv(unz(temp, "11100065.csv"), encoding="UTF-8", stringsAsFactors = FALSE)
  unlink(temp)
  data$X.U.FEFF.REF_DATE <- ceiling_date(ym((data$X.U.FEFF.REF_DATE)), "month") - days(1)
  
  HHBurden <- data %>% 
    filter(Estimates %in% c("Disposable income ", #don't change strs
                            "Debt service ratio"), # interest and principal
           `Seasonal.adjustment` == "Seasonally adjusted at annual rates") %>%
    select(X.U.FEFF.REF_DATE, metric = Estimates, VALUE) %>% 
    mutate(metric = recode(metric,
                           "Disposable income " = "disposable_income",
                           "Debt service ratio" = "debt_service_ratio_hh"))
  HHBurden <- HHBurden %>%
    mutate(date = as.yearqtr(as.Date(X.U.FEFF.REF_DATE, "%m/%d/%Y"))) %>%
    group_by(date, metric) %>%
    select(date, metric, VALUE)
  HHBurden$date <- floor_date(yq((HHBurden$date)), "month") - days(1)
  return(HHBurden)
}

STATCAN_HHCredit <- function(e='dummy') {
  # Pull Household Credit (output:HHCredit) ================================= 
  temp <- tempfile()
  download.file("https://www150.statcan.gc.ca/n1/tbl/csv/36100639-eng.zip",temp)
  data <- read.csv(unz(temp, "36100639.csv"), encoding="UTF-8", stringsAsFactors = FALSE)
  unlink(temp)
  data$X.U.FEFF.REF_DATE <- ceiling_date(ym((data$X.U.FEFF.REF_DATE)), "month") - days(1)
  
  HHCredit <- data %>% 
    rename(metric = "Credit.liabilities.of.households") %>% 
    filter(metric %in% c("Non-mortgage loans",
                         "Mortgage loans"),
           `Seasonality` == "Seasonally adjusted data") %>%
    mutate(metric = recode(metric,
                           "Non-mortgage loans" = "consumer_credit",
                           "Mortgage loans" = "mortgage_credit_residential")) %>% 
    select(X.U.FEFF.REF_DATE, metric, VALUE) 
  
  
  ### get quarter avg amounts  
  HHCredit <- HHCredit %>%
    mutate(date = as.yearqtr(as.Date(X.U.FEFF.REF_DATE, "%m/%d/%Y"))) %>%
    group_by(date, metric) %>%
    summarize(VALUE = last(VALUE))
  HHCredit$date <- floor_date(yq((HHCredit$date)), "month") - days(1)
  return(HHCredit)
}

STATCAN_BC <- function(f='dummy') {
  # Pull Business Credit (output:BCredit) =================================
  
  # Pull Statscan's table on NFC liabilities
  temp <- tempfile()
  download.file("https://www150.statcan.gc.ca/n1/tbl/csv/36100640-eng.zip",temp)
  data <- read.csv(unz(temp, "36100640.csv"), encoding="UTF-8", stringsAsFactors = FALSE)
  unlink(temp)
  data$X.U.FEFF.REF_DATE <- ceiling_date(ym((data$X.U.FEFF.REF_DATE)), "month") - days(1)
  
  Bcredit <- data %>% 
    filter(Seasonality %in% c("Seasonally adjusted data"),
           `Credit.liabilities.of.private.non.financial.corporations` %in% 
             c("Total credit liabilities of private non-financial corporations",
               "Government"), !is.na(VALUE)) 
  
  Bcredit <- Bcredit[-seq(2, NROW(Bcredit), by = 4),]  %>%  #deletes unwanted government figures (the middle one)
    select(X.U.FEFF.REF_DATE, Credit.liabilities.of.private.non.financial.corporations, VALUE) %>%
    group_by(X.U.FEFF.REF_DATE, Credit.liabilities.of.private.non.financial.corporations) %>%
    summarize(VALUE = last(VALUE))  
  
  Bcredit <- Bcredit %>% 
    pivot_wider(names_from = Credit.liabilities.of.private.non.financial.corporations, values_from = VALUE, values_fill = 0) %>% # If no entry exists for trust units for period t, a zero is entered
    mutate(`VALUE` = 
             `Total credit liabilities of private non-financial corporations` - 
             `Government`,
           metric = "business_credit_total") %>% 
    select(X.U.FEFF.REF_DATE, metric, VALUE)
  
  Bcredit <- Bcredit %>%
    mutate(date = as.yearqtr(as.Date(X.U.FEFF.REF_DATE, "%m/%d/%Y"))) %>%
    group_by(date, metric) %>%
    summarize(VALUE = last(VALUE))
  
  Bcredit$date <- floor_date(yq((Bcredit$date)), "month") - days(1)
  return(Bcredit)
}

####ALL FUNCTIONS THAT OUTPUT A DF WITH A COLNAME 'METRIC' SHOULD GO HERE!
COMPUTE_INDICATORS <- function(PMIC_SIZE_file, nfc_dsr_file) {
  statcan_merged <- bind_rows(STATCAN_BC(f), STATCAN_BONDS_ISSUED(a),STATCAN_DTE(c),
                              STATCAN_GDP(b),STATCAN_HHBurden(d),STATCAN_HHCredit(e),
                              PMIC_SIZE(PMIC_SIZE_file), debt_service_ratio_nfc(nfc_dsr_file)) %>% 
    group_by(date)  %>% 
    pivot_wider(names_from = metric, values_from = VALUE)  %>% 
    mutate(total_credit_to_gdp = 100*(mortgage_credit_residential + consumer_credit + business_credit_total)/gdp_nominal,
           debt_to_disposable_income = 100*(mortgage_credit_residential + consumer_credit)/disposable_income,
           business_credit_to_gdp = 100* business_credit_total/gdp_nominal,
           pmic_to_mortgage_credit = 100*(pmic_size_total/mortgage_credit_residential))
  return(statcan_merged)
}

#Flow of funds function
cansim_flow <- function(start.date.cansim){
  data <- get_cansim_vector(c("v62690826","v62691072","v62691154","v62691400","v62691482","v62692138"),start_time = start.date.cansim) %>%
    normalize_cansim_values %>% select(Date, VECTOR, VALUE) %>% rename(date = "Date", variable = "VECTOR", value = "VALUE")
  
  data <- data %>% mutate(variable = case_when(variable == "v62690826" ~ "household_etc_flows_millions",
                                               variable == "v62691072" ~ "corporate_flows_millions",
                                               variable == "v62691154" ~ "nfc_corporate_flows_millions",
                                               variable == "v62691400" ~ "fc_corporate_flows_millions",
                                               variable == "v62691482" ~ "government_flows_millions",
                                               variable == "v62692138" ~ "non_resident_flows_millions"))
  
  gdp <- get_cansim_vector(c("v62305814"),start_time = start.date.cansim) %>%
    normalize_cansim_values %>% select(Date, VECTOR, VALUE) %>% rename(date = "Date", gdp = "VALUE")
  
  data <- left_join(data, gdp, by = "date", all.x = T)
  data <- data %>% mutate(value = value/gdp*100) %>% select(-gdp)
  
  data <- data %>% arrange(date) %>%
    group_by(variable) %>%
    mutate(value = rollapplyr(value, 4, mean, fill = NA, partial = TRUE))
  data <- data %>% select(-VECTOR)
  
  data <- data %>% mutate(date = ceiling_date(as.Date(date), unit = "quarter")-1)
  data <- data %>%  pivot_wider(names_from = variable, values_from = value)
  
  return(data)
  
}

# Household debt to net worth function
hh_cansim <- function(start.date.cansim){
  data <- get_cansim_vector(c("v62698068"),start_time = start.date.cansim) %>%
    normalize_cansim_values %>% select(Date, VECTOR, VALUE) %>% rename(date = "Date", metric = "VECTOR", VALUE = "VALUE")
  
  data <- data %>% mutate(metric = case_when(metric == "v62698068" ~ "debt_to_net_worth_hh"))
  
  data <- data %>% mutate(date = ceiling_date(as.Date(date), unit = "quarter")-1)
  data_hh_cansim <- data %>%  pivot_wider(names_from = metric, values_from = VALUE)
  return(data_hh_cansim)
}

Credit_to_gdp_bis <- function(g="dummy") {
  url <- 'https://www.bis.org/statistics/c_gaps.htm'
  url <- read_html(url) %>% html_nodes("a") %>% html_attr("href") %>% as.data.frame() %>% rename(col = ".")
  url <- url %>% filter(grepl("c_gaps.xlsx",col)) 
  url <- paste0('https://www.bis.org/', url)
  GET(url, write_disk(tf <- tempfile(fileext = ".xlsx")))
  data <- read_xlsx(tf, sheet = "Quarterly Series") %>%
    select("date" = "Back to menu", 
           'actual_credit_to_gdp_bis' 
           = "Credit-to-GDP ratios (actual data) - Canada - Credit from All sectors to Private non-financial sector",
           'trend_credit_to_gdp_bis' 
           = "Credit-to-GDP trend (HP filter) - Canada - Credit from All sectors to Private non-financial sector", 
           'total_credit_to_gdp_gap_bis' = "Credit-to-GDP gaps (actual-trend) - Canada - Credit from All sectors to Private non-financial sector")  
  data <- data[-(1:3),] 
  data[1] <-  as.yearqtr("1948 Q1") + seq(0, length = NROW(data)) / 4
  data[1]  <- floor_date(yq((data$date)), "month") - days(1)
  bis_data <- data %>%
    mutate(actual_credit_to_gdp_bis = as.numeric(actual_credit_to_gdp_bis), 
           trend_credit_to_gdp_bis = as.numeric(trend_credit_to_gdp_bis),
           total_credit_to_gdp_gap_bis = as.numeric(total_credit_to_gdp_gap_bis))
  return(bis_data)
}

# Exempt market mortgage related issuance function
exempt_mortgage <- function(mie.sheet){
  #mie.sheet <- 'T:\\SRWG\\RSR\\SRWL_Targets\\Input Files\\RED_MIE.xlsx'
  df <- read_excel(mie.sheet, sheet = 'Template', skip = 2) %>%
    select(date, AMOUNT) %>% rename(value = "AMOUNT") %>%
    mutate(variable = "exempt_mortgage_issuance", date = as.Date(date))
  
  df <- df %>% mutate(max = if_else(month(date) %in% c(3,6,9,12), date, as.Date(NA))) %>%
    mutate(max = na.locf(max, fromLast = T, na.rm = F)) %>% filter(!is.na(max)) %>% select(-max)
  
  df <- df %>%
    group_by(date = ceiling_date(date, "quarter")-1, variable) %>%
    summarize(value = sum(value, na.rm = T)) 
  df_exempt_mortgage <- df %>% 
    pivot_wider(names_from = variable, values_from = value)
  return(df_exempt_mortgage)
}

EIKON_DATA <- function(eikon_bonds_issuance, eikon_bonds_panel) {
  
  #Import =====================
  #eikon_bonds_issuance <- 'T:\\SRWG\\RSR\\SRWL_Targets\\Input Files\\bonds_gross_issuance_panel.xlsx'
  
  bond_issuance_gross_panel.eikon <- read_xlsx(eikon_bonds_issuance) %>% 
    rename(date = `Issue quarter`) %>%  #Need to re-label for consistency
    mutate(date = ceiling_date(date, "quarters")-1) %>%  
    filter(date > ymd("1990-01-01")) 
  
  #eikon_bonds_panel <- 'T:\\SRWG\\RSR\\SRWL_Targets\\Input Files\\bonds_outstanding_panel.xlsx'
  
  bond_os_panel.eikon <- read_xlsx(eikon_bonds_panel)  %>% 
    mutate(date = ceiling_date(date, "quarters")-1) %>% 
    group_by(date, Sector, `Country Name`, `Rating bucket`) %>% 
    summarize(value = last(value))
  
  
  # Aggregate series ====================
  
  bond_issuance_gross <- bond_issuance_gross_panel.eikon %>% 
    group_by(`date`) %>% 
    summarize(VALUE = sum(value)) %>% 
    ungroup() %>%
    mutate(metric = "bonds_issuance_nfc_gross") %>% 
    select(c(1,3,2))
  
  bond_issuance_gross_hy <- bond_issuance_gross_panel.eikon %>% 
    filter(`Rating bucket` == "Speculative_bucket") %>% 
    group_by(`date`) %>% 
    summarize(VALUE = sum(value)) %>% 
    ungroup() %>%
    mutate(metric = "bonds_issuance_nfc_gross_hy") %>% 
    select(c(1,3,2))
  
  # Combine series into a dataframe ======================
  db.eikon <- rbind(bond_issuance_gross,
                    bond_issuance_gross_hy) %>% mutate(date = as.Date(date))  %>% 
    pivot_wider(names_from = metric, values_from = VALUE)
  
  return(db.eikon)
  
}


# # Pull Refinitiv data in blocks (uses get_data() from 'eikonapir' package)
# # Addresses Eikons cell limit for pulling data
# # Designed only to work with a single-vector data frame (I didn't test other input formats)
# 
# get_data.blocks <- function (ref.list,row_limit,fields){
#   N <- nrow(ref.list) # number or securities to get data for
#   
#   # Initialize loop
#   L <- row_limit
#   i <- 1
#   data.pull <- get_data(paste(unlist(ref.list[1,]),collapse = ","), fields)[-1,]
#   
#   # This loops retrieves data in user-defined blocks and combines them
#   while (i < N){
#     if (N-i < row_limit){ #The final block will be smaller than the others
#       L <- N-i+1
#     }
#     block <- paste(unlist(ref.list[i:(i+L-1),]),collapse = ",")
#     data.pull <- rbind(data.pull,get_data(block, fields))
#     i <- i+L
#   }
#   return(data.pull)
# }
# 
# 
# #Variation of above function with snapshot date field
# get_data.blocks_ss <- function (ref.list,row_limit,fields,snap_date){
#   N <- nrow(ref.list) # number or securities to get data for
#   
#   # Initialize loop
#   L <- row_limit
#   i <- 1
#   data.pull <- get_data(paste(unlist(ref.list[1,]),collapse = ","), fields,parameters = list('SDate' = snap_date))[-1,]
#   
#   # This loops retrieves data in user-defined blocks and combines them
#   while (i < N){
#     if (N-i < row_limit){ #The final block will be smaller than the others
#       L <- N-i+1
#     }
#     block <- paste(unlist(ref.list[i:(i+L-1),]),collapse = ",")
#     data.pull <- rbind(data.pull,get_data(block, fields,parameters = list('SDate' = snap_date)))
#     i <- i+L
#   }
#   return(data.pull)
# }
# 
# 
# # Convert a dataframe list of characters into an atomic list of characters
# #Eg: Useful for filtering bond data based on a list of specific ISINs (ie: dplyr:: filter(field %in% list))
# vector.df_to_atomic <- function(df_vector){
#   x<-paste(unlist(df_vector), sep = ",")
#   return(x)
# }
#################################################################################


# output_final_df_long <- function(boc_prices_url,PMIC_SIZE_file,nfc_dsr_file,url_iiroc_marketplace,
#                      crea_housing_file,eikon_bonds_issuance,eikon_bonds_panel,
#                      start.date.cansim,tsx_mig_file,iiroc_mrts_file,mie.sheet) {
#   df <- purrr::reduce(list(BOC_DATA(boc_prices_url),
#                            COMPUTE_INDICATORS(PMIC_SIZE_file,nfc_dsr_file),
#                            IIROC_df_MarketplaceStats(url_iiroc_marketplace),
#                            CREA_HOUSING(crea_housing_file), hh_cansim(start.date.cansim), 
#                            EIKON_DATA(eikon_bonds_issuance, eikon_bonds_panel),  
#                            Credit_to_gdp_bis(g), TSX_MiG(tsx_mig_file),
#                            IIROC_DF_MRTS(iiroc_mrts_file), 
#                            cansim_flow(start.date.cansim), exempt_mortgage(mie.sheet))
#                       ,dplyr::left_join, by = 'date') 
#   
#   df_long <- df  %>% 
#     filter(as.Date(date) >= as.Date('2000-01-01')) %>% 
#     select(c("date",sort(names(df[,-1])))) %>%
#     pivot_longer(cols=c(2:47), names_to='variable', values_to='value') %>% 
#     group_by(date) %>%
#     mutate('file.name' = 'rsr_srwl.ts.csv') %>%
#     rename(calendar.quarter = date) %>%
#     select(file.name, calendar.quarter, variable, value)
#   
#   #write_csv(df_long,'C:\\Users\\MjaenCortes\\Desktop\\R Projects\\John Project\\rsr_srwl.ts.csv')
#   #write_delim(df,paste0(output.files,"archive/rsr_srwl.ts", Sys.Date(),".csv"),delim = ",")    
#   message("**********Databases joined and saved (db.all, db.all_wide)****************")
#   
#   return(df_long)
# }

# crypto_function <- function(){
#   ### Pull crypto market cap
#   list_coins <- crypto_list(only_active = ) %>%
#     arrange(rank)
#   
#   #Capture top 150 coin histories
#   list_coins1 <- list_coins[1:75,]
#   list_coins2 <- list_coins[76:150,]
#   #list_coins3 <- list_coins[151:200,]
#   
#   #Pull data in two pulls (with a 5 sec break in between)
#   crypto.data1 <- crypto_history(list_coins1, start_date = "20170101")
#   message("********** Please wait for 5 seconds ****************")
#   Sys.sleep(5)
#   crypto.data2 <- crypto_history(list_coins2, start_date = "20170101")
#   #crypto.data3 <- crypto_history(list_coins3, start_date = "20170101")
#   message("********** Data scraping COMPLETE ****************")
#   Sys.sleep(5)
#   gc()
#   
#   #Combine and rank by market cap
#   crypto.data <- rbind(crypto.data1,crypto.data2) %>% 
#     arrange("market cap") %>%
#     mutate(time_open = floor_date(time_open,"days")) %>%
#     select(c("date" = "time_open","symbol","market_cap","volume","high","low")) %>% 
#     mutate(date = as.POSIXct(date),
#            date = as.Date(date))
#   
#   stable.data <- crypto.data %>% 
#     filter(high <1.01,
#            low > 0.99)
#   
#   list <- crypto.data %>% group_by(symbol) %>% summarize(close = mean(close, na.rm = T)) %>% filter(close > 0.95 & close < 1.05)
#   
#   crypto.all <- crypto.data %>% 
#     group_by(date) %>% 
#     summarize(market_cap = sum(market_cap), volume = sum(volume)) %>% 
#     ungroup() %>% 
#     mutate(date = floor_date(date,"quarter") - days(1)) %>%
#     group_by(date) %>% 
#     summarize_all(last) %>% 
#     ungroup() %>%
#     rename(crypto_market_cap = market_cap, crypto_volume = volume) %>% 
#     gather("metric",VALUE,2:3)
#   
#   
#   stable.all <- stable.data %>% 
#     group_by(date) %>% 
#     summarize(market_cap = sum(market_cap), volume = sum(volume)) %>% 
#     ungroup() %>% 
#     mutate(date = floor_date(date,"quarter") - days(1)) %>%
#     group_by(date) %>% 
#     summarize_all(last) %>% 
#     ungroup() %>% 
#     rename(stablecoin_market_cap = market_cap, stablecoin_volume = volume) %>%
#     gather("metric",VALUE,2:3)
#   
#   #Combine for SRWG and RC Dashboard
#   db.crypto <- rbind(crypto.all, stable.all)
#   
#   return(db.crypto)
# }

output_final_df_long <- function(db.boc, IIROC_DF_MTRS, df_TSX_MiG, 
                                 housing_data_df, IIROC_Equity_df, statcan_merged,
                                 data, data_hh_cansim, bis_data, 
                                 df_exempt_mortgage, db.eikon) {
  
  df <- purrr::reduce(list(db.boc, IIROC_DF_MTRS, df_TSX_MiG, 
                           housing_data_df, IIROC_Equity_df, statcan_merged,
                           data, data_hh_cansim, bis_data, 
                           df_exempt_mortgage, db.eikon)
                      ,dplyr::left_join, by = 'date') 
  
  df_long <- df  %>% 
    filter(as.Date(date) >= as.Date('2000-01-01')) %>% #be careful as current format is -days(1)
    select(c("date",sort(names(df[,-1])))) %>%
    pivot_longer(cols=c(2:47), names_to='variable', values_to='value') %>%  #cols=c(2:51) w crypto data
    group_by(date) %>%
    mutate('file.name' = 'rsr_srwl.ts.csv') %>%
    rename(calendar.quarter = date) %>%
    select(file.name, calendar.quarter, variable, value)
  
  #df_long$calendar.quarter <- df_long$calendar.quarter + days(1)
  
  #write_csv(df_long,'C:\\Users\\MjaenCortes\\Desktop\\R Projects\\John Project\\rsr_srwl.ts.csv')
  #write_delim(df,paste0(output.files,"archive/rsr_srwl.ts", Sys.Date(),".csv"),delim = ",")    
  message("**********Databases joined and saved (db.all, db.all_wide)****************")
  
  return(df_long)
}

output_func <- function(data, output_path){
  write_csv(data, output_path)
  output_path ## you must return an output path so that the file can be tracked. 
}

