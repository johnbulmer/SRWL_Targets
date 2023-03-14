########################################
#
# MAIN PROGRAM
#
# Author: Marcos Jaen Cortes, Regulatory Strategy and Research
# Created Feb 2023
#
########################################
# Load packages required to define the pipeline:


library(targets)
library(tarchetypes)

#### Parameters
## Set import criteria to avoid number trimming
options(scipen = 999, digits = 12, max.print=10000, tidyverse.quiet = TRUE)

# Load packages required to define the pipeline:
pacman::p_load(tidyverse, cansim, smooth, mFilter, forcats, stringr, janitor, readxl, xts, data.table,
               tibbletime, zoo, lubridate, Rblpapi, gridExtra, seasonal, rvest, httr, curl, tinytest, 
               readxl, httr, purrr, rio, timeDate, bit, dplyr, PerformanceAnalytics, DescTools, crypto2) 
options(scipen = 999, digits = 12, max.print=10000, tidyverse.quiet = TRUE)


# Set target options:
tar_option_set(packages = c("janitor", "PerformanceAnalytics", "scales", "data.table", 
                            "readxl", "cansim",
                            "lubridate", "tidyverse", "DescTools",
                            "withr", "fstcore", "targets", "testthat", 
                            'PerformanceAnalytics', 'DescTools', 'rvest', 'crypto2'),
               format = "fst_dt")

# tar_make_clustermq() configuration (okay to leave alone):
options(clustermq.scheduler = "multiprocess")

# tar_make_future() configuration (okay to leave alone):
future::plan(future.callr::callr)


# Load Functions File
setwd("T://SRWG//RSR//SRWL_Targets")
source("R/functions.R")

# Set other inputs (Output)
outfile_name <- "rsr_srwl.ts.csv"
outfile_path <-  paste0("T://SRWG//RSR//SRWL_Targets//",outfile_name) %>% as.character()


## TARGET LIST  ----------------------------


list(
  
  #Data input files
  tar_target(iiroc_mrts_file, 
             'T:\\SRWG\\RSR\\SRWL_Targets\\Input Files\\IIROC_DF_MTRS2WebStats.xlsx', 
             format = "file"),
  
  tar_target(tsx_mig_file, 
             'T:\\SRWG\\RSR\\SRWL_Targets\\Input Files\\TSX_MIG.xlsx', 
             format = "file"),
  
  tar_target(crea_housing_file, 
             'T:\\SRWG\\RSR\\SRWL_Targets\\Input Files\\Seasonally Adjusted.xlsx', 
             format = "file"),
  tar_target(nfc_dsr_file, 
             'T:\\SRWG\\RSR\\Script New\\Input files\\CapIQ_financial_statements_data.rds', 
             format = "file"),
  
  tar_target(PMIC_SIZE_file, 
             'T:\\SRWG\\RSR\\SRWL_Targets\\Input Files\\PMIC_Size.xlsx', 
             format = "file"),
  
  tar_target(mie.sheet, 
             'T:\\SRWG\\RSR\\SRWL_Targets\\Input Files\\RED_MIE.xlsx', 
             format = "file"),
  
  tar_target(eikon_bonds_issuance, 
             'T:\\SRWG\\RSR\\SRWL_Targets\\Input Files\\bonds_gross_issuance_panel.xlsx', 
             format = "file"),
  
  tar_target(eikon_bonds_panel, 
             'T:\\SRWG\\RSR\\SRWL_Targets\\Input Files\\bonds_outstanding_panel.xlsx', 
             format = "file"),
  
 

  #Load data functions
  tar_target(IIROC_Equity_df, 
             IIROC_df_MarketplaceStats("https://www.iiroc.ca/sections/markets/reports-statistics-and-other-information/reports-market-share-marketplace")),
  tar_target(IIROC_DF_MTRS, IIROC_DF_MRTS(iiroc_mrts_file)),
  tar_target(df_TSX_MiG, TSX_MiG(tsx_mig_file)),
  tar_target(housing_data_df, CREA_HOUSING(crea_housing_file)),
  tar_target(nfc_dsr, debt_service_ratio_nfc(nfc_dsr_file)),
  tar_target(PMIC_Size, PMIC_SIZE(PMIC_SIZE_file)),
  tar_target(db.boc, 
             BOC_DATA('https://www.bankofcanada.ca/rates/price-indexes/bcpi/')),
  
  tar_target(Bonds_Issued, STATCAN_BONDS_ISSUED('dummy')),
  tar_target(gdp.n, STATCAN_GDP('dummy')),
  tar_target(dte, STATCAN_DTE('dummy')),
  tar_target(HHBurden, STATCAN_HHBurden('dummy')),
  tar_target(HHCredit, STATCAN_HHCredit('dummy')),
  tar_target(data, cansim_flow("1990-01-01")),
  tar_target(data_hh_cansim, hh_cansim("1990-01-01")),
  tar_target(bis_data, Credit_to_gdp_bis('dummy')),
  tar_target(df_exempt_mortgage, exempt_mortgage(mie.sheet)),
  tar_target(db.eikon, EIKON_DATA(eikon_bonds_issuance, eikon_bonds_panel)),
  #tar_target(db.crypto, crypto_function()),
  
  
  # Create df with indicators 
  tar_target(
    statcan_merged,
    COMPUTE_INDICATORS(
      PMIC_SIZE_file = PMIC_SIZE_file,
      nfc_dsr_file = nfc_dsr_file
    )),
  # Combine all dfs under one df
  tar_target(df_long, output_final_df_long(db.boc = db.boc, 
                                           IIROC_DF_MTRS = IIROC_DF_MTRS, 
                                           df_TSX_MiG = df_TSX_MiG, 
                                           housing_data_df = housing_data_df, 
                                           IIROC_Equity_df = IIROC_Equity_df, 
                                           statcan_merged = statcan_merged,
                                           data = data, 
                                           data_hh_cansim = data_hh_cansim, 
                                           bis_data =  bis_data, 
                                           df_exempt_mortgage = df_exempt_mortgage, 
                                           db.eikon = db.eikon)), 
  
  
  # Write output
  tar_target(output,
             output_func(df_long, outfile_path), format = 'file')
  
)

