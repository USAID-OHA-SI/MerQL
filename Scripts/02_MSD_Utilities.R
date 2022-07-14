##  PROJECT: SI Support for Nigeria
##  AUTHOR:  Baboyma Kagniniwa | USAID
##  PURPOSE: Utility functions
##  LICENCE: MIT
##  DATE:    2021-10-08
##  UPDATED: 2022-07-14

# Libraries ----

  library(tidyverse)
  library(gophr)
  library(glamr)
  library(janitor)
  # library(DBI)
  # library(uuid)

  source("./Scripts/00_Utilities.R")

# GLOBALS ----

  dir_merdata <- glamr::si_path("path_msd")
  dir_data <- "Data"
  dir_dataout <- "Dataout"
  dir_graphics <- "Graphics"

  #b_url <- "https://datim.org"       # Live data
  b_url <- "https://final.datim.org" # Frozen data

  cntry <- "Nigeria"

  cntry_uid <- get_ouuid(operatingunit = cntry)

  conn <- db_connection(db_name = pg_database())

# DATA EXTRACTION ----

  # glamr::pano_extract_msds(operatingunit = cntry,
  #                          archive = TRUE,
  #                          dest_path = dir_merdata)

# FUNCTIONS ----

  #' @title Pull Data Elements from MSD Dataset
  #'
  msd_orgs <- function(df_msd, level = NULL) {
    # Notification
    if (interactive())
      usethis::ui_info("INFO - Extracting Organization Details from MSD ...")

    df_outable <- glamr::get_outable(
      username = datim_user(),
      password = datim_pwd())

    df_orgs_sites <- df_msd %>%
      select(any_of(str_msd_sites$org_units)) %>%
      distinct()

    df_orgs_sites <- df_outable %>%
      select(!ends_with("lvl")) %>%
      rename_with(~str_replace(., "_", "")) %>%
      right_join(df_orgs_sites,
                 by = c("operatingunituid", "operatingunit", "country"))

    return(df_orgs_sites)
  }

  #' @title Pull Mechanisms Details from MSD
  #'
  msd_mechanisms <- function(df_msd, level = "all") {
    # Notification
    if (interactive())
      usethis::ui_info("INFO - Extracting mechanisms details from MSD ...")

    cols_mech <- str_msd_sites$mechanisms

    if (!all(cols_mech %in% names(df_msd)))
      usethis::ui_stop(glue::glue("ERROR - MSD Table is missing required columns: {paste(cols_fact, collapse=', ')}"))

    df_mechs <- df_msd %>%
      select(one_of(str_msd_sites$mechanisms)) %>%
      distinct() %>%
      msd_clean_mechs()

    if (nrow(df_mechs) != nrow(distinct(df_mechs, mech_code)))
        usethis::ui_warn("WARNING - Mechanisms from the MSD seems to have duplicate records")

    return(df_mechs)
  }

  #' @title Pull Partners Details from Mechanisms
  #'
  #'
  msd_partners <- function(df_msd) {
    df_msd %>%
      select(any_of(str_msd_sites$mech_partners)) %>%
      distinct()
  }

  #' @title Pull Awards Details from Mechanisms
  #'
  #'
  msd_awards <- function(df_msd) {
    df_msd %>%
      select(any_of(str_msd_sites$mech_awards)) %>%
      distinct()
  }

  #' @title Clean Mechanisms / TBD
  #'
  msd_clean_mechs <- function(df_mechs) {
    # Notification
    if (interactive())
      usethis::ui_info("INFO - Cleaning TBDs & Placeholders in Mechanism data ...")

    df_mechs %>%
      mutate(
        award_number = case_when(
          str_detect(mech_name, "Placeholder") ~ paste0("TBD - ", mech_code, " ", operatingunit, " ", funding_agency),
          TRUE ~ award_number
        ),
        prime_partner_name = case_when(
          str_detect(mech_name, "Placeholder") ~ paste0("TBD - ", mech_code, " ", operatingunit, " ", funding_agency),
          TRUE ~ prime_partner_name
        ),
        prime_partner_duns = case_when(
          str_detect(mech_name, "Placeholder") ~ paste0("TBD - ", mech_code, " ", operatingunit, " ", funding_agency),
          TRUE ~ prime_partner_duns
        ),
        prime_partner_uei = case_when(
          str_detect(mech_name, "Placeholder") ~ paste0("TBD - ", mech_code, " ", operatingunit, " ", funding_agency),
          TRUE ~ prime_partner_uei
        ),
        mech_name = case_when(
          str_detect(mech_name, "Placeholder") ~ paste0("TBD - ", mech_code, " ", operatingunit, " ", funding_agency),
          TRUE ~ mech_name
        )
      )
  }

  #' @title Pull Data Elements from MSD Dataset
  #'
  msd_dataelements <- function(df_msd) {
    # Notification
    if (interactive())
      usethis::ui_info("INFO - Extracting Data Elements details from MSD ...")

    # DataElement Variables
    cols_elmts <- c(
      "indicator",
      "numeratordenom",
      "indicatortype",
      "disaggregate",
      "standardizeddisaggregate")

    df_msd %>%
      select(all_of(cols_elmts)) %>%
      distinct() %>%
      rowwise() %>%
      mutate(
        dataelement = msd_build_elements(
          ind = indicator,
          ndenom = numeratordenom,
          itype = indicatortype,
          disagg = disaggregate)) %>%
      ungroup() %>%
      relocate(dataelement, .before = standardizeddisaggregate)
  }

  #' @title Build DataElements
  #'
  msd_build_elements <- function(ind, ndenom, itype, disagg) {

    dataelement <- base::paste(
      purrr::discard(c(ndenom,
                       ifelse(itype == "Not Applicable", "NA", itype),
                       ifelse(str_detect(disagg, "^Total.*"), NA_character_, disagg)),
                     is.na),
      collapse = ", "
    )

    base::paste0(ind, " (", dataelement, ")")
  }

  #' @title Data Elements
  #'
  msd_dataelements2 <- function(df_msd, replace = F) {

    cols <- c("indicator", "numeratordenom", "indicatortype", "disaggregate")

    if (!all(cols %in% base::names(df_msd)))
      usethis::ui_stop(glue::glue("Some of the required columns are missing: {paste(cols, collapse = ', ')}"))

    df <- df_msd %>%
      mutate(
        indicatortype = case_when(
          indicatortype == "Not Applicable", "NA",
          TRUE ~ indicatortype
        ),
        disaggregate = case_when(
          str_detect(disaggregate, "^Total.*") ~ NA_character_,
          TRUE ~ disaggregate
        ),
        dataelement = paste(
          purrr::discard(c(numeratordenom, indicatortype, disaggregate), is.na),
          collapse = ", "),
        dataelement = paste0(indicator, " (", element, ")")
      ) %>%
      relocate(element, .after = disaggregate)

    if (replace) {
      df <- df %>% dplyr::select(!all_of(cols))
    }

    return(df)
  }

  #' @title Get Fact Table
  #'
  msd_fact <- function(df_msd, df_elmts = NULL) {
    # Notification
    if (interactive())
      usethis::ui_info("INFO - Extracting fact table from MSD ...")

    # Fact Table Variables
    cols_fact <- c(
      "fiscal_year",  # => Used to pivot long
      "orgunituid",   # => Join to Org Hierarchy
      "mech_code",    # => Join to Attributes Option Combos - Awards & Partners
      "indicator",      # DataElement key
      "numeratordenom", # DataElement key
      "indicatortype",  # DataElement key
      "disaggregate",   # DataElement key
      "standardizeddisaggregate",
      "categoryoptioncomboname",
      "targets",
      "qtr1",
      "qtr2",
      "qtr3",
      "qtr4",
      "cumulative"
    )

    if (!all(cols_fact %in% names(df_msd)))
      usethis::ui_stop(glue::glue("ERROR - MSD Table is missing required columns: {paste(cols_fact, collapse=', ')}"))

    # DataElement Variables
    cols_disaggs1 <- c("indicator", "numeratordenom",
                       "indicatortype", "disaggregate")

    cols_disaggs2 <- c(cols_disaggs1, "standardizeddisaggregate")

    cols_elmts <- c(cols_disaggs2, "dataelement")

    if (base::is.null(df_elmts))
      df_elmts <- msd_elements(df_msd)

    if (!is.null(df_elmts) & !all(cols_elmts %in% names(df_elmts)))
      usethis::ui_stop(glue::glue("ERROR - Reference Table is missing required columns: {paste(cols_elmts, collapse=', ')}"))

    # Distinct Data Elements
    df_elements <- df_elmts %>%
      select(all_of(cols_elmts)) %>%
      distinct()

    # Drop Reference Columns
    df_fact <- df_msd %>%
      select(all_of(cols_fact)) %>%
      left_join(df_elements, by = cols_disaggs2) %>%
      select(!all_of(cols_disaggs1)) %>%
      relocate(dataelement, .before = standardizeddisaggregate)

    return(df_fact)
  }

  #' @title Reshape MSD Fact Table
  #'
  msd_reshape_fact <- function(df_msd) {
    # Notification
    if (interactive())
      usethis::ui_info("INFO - Reshaping fact table into long format ...")

    df_msd %>%
      pivot_longer(cols = c(starts_with("qtr"), "cumulative", "targets"),
                   names_to = "value_type",
                   values_to = "value",
                   values_drop_na = T) %>%
      mutate(
        period = case_when(
          str_detect(value_type, "^qtr") ~ paste0("Q", str_sub(value_type, -1)),
          TRUE ~ ""
        ),
        period = paste0("FY", str_sub(fiscal_year, 3,4), period),
        period_type = case_when(
          str_detect(period, "Q\\d{1}$") ~ "Quarter",
          TRUE ~ "Fiscal Year"
        ),
        value_type = case_when(
          value_type != "targets" ~ "results",
          TRUE ~ value_type
        )
      ) %>%
      relocate(period, period_type, .after = fiscal_year)
  }

  #' @title Unpack MSD Dataset
  #'
  msd_split <- function(df_msd, reshape = FALSE) {
    # Notification
    if (interactive())
      usethis::ui_info("INFO - Spliting MSD Dataset into dimensions and fact ...")

    df <- list()

    df$orgs <- msd_orgs(df_msd)

    df$de <- msd_dataelements(df_msd)

    df$mechs <- msd_mechanisms(df_msd)

    df$fact <- msd_fact(df_msd, df_elmts = df$de)

    if (reshape) {
      df$fact <- df$fact %>% msd_reshape_fact()
    }

    return(df)
  }

# DATA ----

## Reference Data sets ----

  # Metadata - OU Org Hierarchy with UIDs
  df_outable <- glamr::get_outable(username = datim_user(), password = datim_pwd())

  # Metadata - OU Org Hierarchy with UIDs
  df_orglevels <- glamr::get_levels(username = datim_user(), password = datim_pwd())

  # Metadata - Data Elements
  #df_dt_elts <- datim_resources(res_name = "Data Elements", dataset = T)
  #df_dt_elts <- datim_dataements()

  # MSD - Site x IM ---
  df_sites <- glamr::return_latest(
      folderpath = glamr::si_path(),
      pattern = paste0("Site_IM_FY20.*_", cntry)
    ) %>%
    gophr::read_msd()

  # Clean Data Elements
  df_sites <- df_sites %>%
    mutate(
      source_name = case_when(
        indicator == "OVC_HIVSTAT" &
          standardizeddisaggregate == "Total Numerator" &
          source_name != "Derived" ~ "Derived",
        TRUE ~ source_name
      ),
      modality = case_when(
        indicator == "HTS_RECENT" &
          standardizeddisaggregate == "Total Numerator" &
          !is.na(modality) ~ NA_character_,
        TRUE ~ modality
      ),
      statushiv = case_when(
        indicator %in% c("OVC_HIVSTAT", "TB_STAT_POS") &
          standardizeddisaggregate == "Total Numerator" &
          !is.na(statushiv) ~ NA_character_,
        indicator == "TX_PVLS" &
          standardizeddisaggregate == "Total Denominator" &
          !is.na(statushiv) ~ NA_character_,
        TRUE ~ statushiv
      )
    )

# DATIM Data Schema ----

  # DIMENSIONS
  #
  # WHERE
  # Org Units
  # Eg: Clinical Facility, Community Site, or OU Level
  #
  # WHAT
  # Data Elements: Indicators (N|D, DSD|TA|etc, Disaggregate)
  # Eg: Number of HTC tests for Females 1-4
  #
  # WHEN
  # Reporting Period - Quarter
  # Eg: 2021 qtr2 or January through March 2021
  #
  # WHO
  # Funding mechanism (attribute option combination)
  # Eg: USAID funds RISE which is implemented by FHI360
  #
  # VALUE (MEASUREMENT)
  # Value (Targets, Qtr1-4, Cumulative)
# COLUMN Data Types ----

  cols_msd_sites <- c(
    "period"                 = "varchar(6)",    # ID
    "period_type"            = "varchar(11)",   # ID
    "orgunituid"             = "varchar(11)",   # ID
    "sitename"               = "varchar(200)",
    "facilityuid"            = "varchar(11)",
    "facility"               = "varchar(200)",
    "sitetype"               = "varchar(20)", #9
    "communityuid"           = "varchar(11)",
    "community"              = "varchar(200)",
    "psnuuid"                = "varchar(11)",
    "psnu"                   = "varchar(200)",
    "snuprioritization"      = "varchar(50)",
    "snu1uid"                = "varchar(11)",
    "snu1"                   = "varchar(50)",
    "typemilitary"           = "char(1)",
    "dreams"                 = "char(1)",
    "operatingunit"          = "varchar(32)",
    "operatingunituid"       = "varchar(11)",
    "operatingunitiso"       = "char(3)",
    "country"                = "varchar(50)",
    "countryuid"             = "varchar(11)",
    "countryiso"             = "char(3)",

    "mech_code"              = "varchar(50)",              # ID
    "mech_name"              = "varchar(200)",
    "award_number"           = "varchar(250)",
    "prime_partner_name"     = "varchar(200)",
    "prime_partner_duns"     = "varchar(100)", #30
    "prime_partner_uei"      = "varchar(100)", #30
    "funding_agency"         = "varchar(100)", #30

    "indicator"              = "varchar(50)",                # ID
    "numeratordenom"         = "char(1)",
    "indicatortype"          = "varchar(20)", #4
    "disaggregate"           = "varchar(100)", #30
    "standardizeddisaggregate" = "varchar(100)", #30
    "dataelementuid"           = "varchar(11)",                # ID
    "dataelement"              = "varchar(100)",
    "categoryoptioncombouid"   = "varchar(11)",   # ID (Second)
    "categoryoptioncomboname"  = "varchar(500)",   # ID (Second)
    "ageasentered"             = "varchar(25)",
    "age_2018"                 = "varchar(25)",
    "age_2019"                 = "varchar(25)",
    "trendscoarse"             = "varchar(25)",
    "sex"                      = "varchar(11)", #6
    "statushiv"                = "varchar(25)",
    "statustb"                 = "varchar(25)",
    "statuscx"                 = "varchar(25)",
    "hiv_treatment_status"     = "varchar(25)",
    "otherdisaggregate"        = "varchar(500)",
    "otherdisaggregate_sub"    = "varchar(500)",
    "modality"                 = "varchar(60)",

    "fiscal_year"              = "char(4)",             # ID - Reshape long and use period as ID
    "targets"                  = "decimal(36,2)",
    "qtr1"                     = "decimal(36,2)",
    "qtr2"                     = "decimal(36,2)",
    "qtr3"                     = "decimal(36,2)",
    "qtr4"                     = "decimal(36,2)",
    "cumulative"               = "decimal(36,2)",
    "source_name"              = "varchar(60)",
    "value_type"               = "varchar(7)",
    "value"                    = "decimal(36,2)"
  )


# SITE x IM Data Structure ----

  str_msd_sites <- list(
    # WHEN
    "periods" = c(
      "fiscal_year"
    ),
    # WHERE
    "org_units" = c(
      "orgunituid",                 # ID
      "sitename",
      "facilityuid",
      "facility",
      "sitetype",
      "communityuid",
      "community",
      "psnuuid",
      "psnu",
      "snuprioritization",
      "typemilitary",
      "dreams",
      "snu1",
      "snu1uid",
      "countryuid",
      "country",
      "operatingunituid",
      "operatingunit"
    ),
    "org_sites" = c(
      "orgunituid",                 # ID
      "sitename",
      "facilityuid",
      "facility",
      "sitetype",
      "typemilitary",
      "communityuid",
      "snu1uid",
      "psnuuid",
      "countryuid",
      "operatingunituid"
    ),
    "org_comms" = c(
      "communityuid",
      "community",
      "psnuuid"
    ),
    "org_psnus" = c(
      "psnuuid",
      "psnu",
      "snuprioritization",
      "dreams",
      "snu1uid"
    ),
    "org_snu1s" = c(
      "snu1uid",
      "snu1",
      "countryuid"
    ),
    "org_countries" = c(
      "countryuid",
      "countryiso",
      "country",
      "operatingunituid"
    ),
    "org_ous" = c(
      "operatingunituid",
      "operatingunitiso",
      "operatingunit"
    ),
    # WHAT: dataElements
    "dataelements" = c(
      "indicator",                  # ID
      "numeratordenom",             # ID
      "indicatortype",              # ID
      "disaggregate",               # ID
      "standardizeddisaggregate",
      "dataelement",                # Derived
      "categoryoptioncomboname",    # ID
      "ageasentered",
      "sex",
      "statushiv",
      "statustb",
      "statuscx",
      "hiv_treatment_status",
      "otherdisaggregate",
      "otherdisaggregate_sub",
      "modality",
      "source_name"
    ),
    "indicators" = c(
      "indicator",
      "numeratordenom",
      "indicatortype",
      "disaggregate",
      "dataelement",                # ID
      "standardizeddisaggregate",   # ID
      "modality",
      "source_name"
    ),
    # WHAT: CategoryOptionCombos
    "disaggregates" = c(
      "indicator",
      "numeratordenom",
      "indicatortype",
      "disaggregate",
      "disaggregate",               # ID
      "standardizeddisaggregate",   # ID
      "otherdisaggregate",
      "otherdisaggregate_sub",
      "modality",
      "source_name"
    ),
    # AGES
    "age_disaggregates" = c(
      "fiscal_year",
      "indicator",
      "ageasentered",
      "age_2018",
      "age_2019",
      "trendscoarse"
    ),
    "categoryoptioncombos" = c(
      "indicator",
      "numeratordenom",
      "indicatortype",
      "categoryoptioncomboname",    # ID
      "ageasentered",
      "sex",
      "statushiv",
      "statustb",
      "statuscx",
      "hiv_treatment_status",
      "otherdisaggregate",
      "otherdisaggregate_sub",
      "source_name"
    ),
    # WHO: Mechs - AttributesOptionsCombo
    "mechanisms" = c(
      "mech_code",                  # ID
      "mech_name",
      "award_number",
      "prime_partner_name",
      "prime_partner_duns",
      "prime_partner_uei",
      "funding_agency",
      "operatingunit"
    ),
    "mech_partners" = c(
      "prime_partner_uei",          # ID
      "prime_partner_duns",         # ID
      "prime_partner_name"
    ),
    "mech_awards" = c(
      "mech_code",                  # ID
      "mech_name",
      "award_number",               # ID
      "prime_partner_uei",
      "operatingunit",
      "funding_agency"
    ),
    # VALUE
    "values" = c(
      "targets",
      "qtr1",
      "qtr2",
      "qtr3",
      "qtr4",
      "cumulative"
    ),
    # OTHER
    "others" = c(),
    # Slimmed Version of MSD,
    # Similar to Flat Files exported from EMRs
    "data" = c(
      "fiscal_year",  # => Used to reshape reshape / pivot
      "orgunituid",   # => Join to Org Hierarchy
      "mech_code",    # => Join to AttributesOptionsCombo tbl
      "indicator",
      "numeratordenom", # DataElement key
      "indicatortype",  # DataElement key
      "disaggregate",   # DataElement key
      "dataelement",
      "standardizeddisaggregate",
      "categoryoptioncomboname",
      "targets",
      "qtr1",
      "qtr2",
      "qtr3",
      "qtr4",
      "cumulative"
    ),
    "data_long" = c(
      "period",       # => Used to reshape reshape / pivot
      "orgunituid",   # => Join to Org Hierarchy
      "mech_code",    # => Join to AttributesOptionsCombo tbl
      "dataelement",
      "standardizeddisaggregate",
      "categoryoptioncomboname",
      "value_type",
      "value"
    ),
    # Columns for cleaned version
    "cleaned" = c()
  )

# Flatfile => db_output.csv or txt, json, data long
# SQL View => hyperfile, data long
# SQL View => hyperfile + geospatial (site + psnu + OU/country)

# NORMALISE ----

  ## Org Hierarchy ----

  # Orgs Table Names

  tbl_orgs <- "msd_orgunits"

  tbl_org_sites <- "msd_org_sites"
  tbl_org_comms <- "msd_org_communities"
  tbl_org_psnus <- "msd_org_psnus"
  tbl_org_snu1s <- "msd_org_snu1s"
  tbl_org_cntries <- "msd_org_countries"
  tbl_org_ous <- "msd_org_ous"
  tbl_org_levels <- "msd_org_levels"

  df_orgs_sites <- df_sites %>%
    select(any_of(str_msd_sites$org_units)) %>%
    distinct()

  df_orgs_sites <- df_outable %>%
    select(!ends_with("lvl")) %>%
    rename_with(~str_replace(., "_", "")) %>%
    right_join(df_orgs_sites,
               by = c("operatingunituid", "operatingunit", "country")) %>%
    relocate(starts_with("country"), starts_with("operatingunit"),
             .after = last_col())

  df_orgs_sites %>% glimpse()

  nrow(df_orgs_sites) == nrow(distinct(df_orgs_sites, orgunituid))

  # All org units
  df_orgs_sites %>%
    db_create_table(tbl_orgs, ., conn,
                    meta = cols_msd_sites,
                    pkeys = "orgunituid",
                    overwrite = T)

  # All sites
  df_orgs_sites %>%
    select(all_of(str_msd_sites$org_sites)) %>%
    distinct() %>%
    db_create_table(tbl_org_sites, ., conn,
                    meta = cols_msd_sites,
                    pkeys = "orgunituid",
                    overwrite = T)

  # All Communities
  df_orgs_sites %>%
    filter(community != "Data reported above Community Level") %>%
    select(all_of(str_msd_sites$org_comms)) %>%
    distinct() %>%
    db_create_table(tbl_org_comms, ., conn,
                    meta = cols_msd_sites,
                    pkeys = "communityuid",
                    overwrite = T)

  # All PSNUs
  df_orgs_sites %>%
    select(all_of(str_msd_sites$org_psnus)) %>%
    distinct() %>%
    db_create_table(tbl_org_psnus, ., conn,
                    meta = cols_msd_sites,
                    pkeys = "psnuuid",
                    overwrite = T)

  # All SNU1s
  df_orgs_sites %>%
    select(all_of(str_msd_sites$org_snu1s)) %>%
    distinct() %>%
    db_create_table(tbl_org_snu1s, ., conn,
                    meta = cols_msd_sites,
                    pkeys = "snu1uid",
                    overwrite = T)

  # All Countries
  df_outable %>%
    rename_with(~str_replace(., "_", "")) %>%
    select(all_of(str_msd_sites$org_countries)) %>%
    distinct() %>%
    db_create_table(tbl_org_cntries, ., conn,
                    meta = cols_msd_sites,
                    pkeys = "countryuid",
                    overwrite = T)

  # All OUs
  df_outable %>%
    rename_with(~str_replace(., "_", "")) %>%
    select(all_of(str_msd_sites$org_ous)) %>%
    distinct() %>%
    db_create_table(tbl_org_ous, ., conn,
                    pkeys = "operatingunituid",
                    overwrite = T)

  # All OU Levels
  df_outable %>%
    select(ends_with(c("uid", "lvl"))) %>%
    rename_with(~str_replace(., "_", "")) %>%
    pivot_longer(cols = ends_with("lvl"),
                 names_to = "label",
                 values_to = "level") %>%
    mutate(label = str_remove(label, "lvl$")) %>%
    db_create_table(tbl_org_levels, ., conn,
                    meta = cols_msd_sites,
                    pkeys = c("operatingunituid", "countryuid", "label", "level"),
                    overwrite = T)

  remove(df_orgs_sites)


  ## Mechanisms ----

  # Table Names
  tbl_mechanisms <- "msd_mechanisms"
  tbl_mechs_awards <- "msd_mech_awards"
  tbl_mechs_partners <- "msd_mech_partners"

  df_mechs <- df_sites %>% msd_mechanisms()

  df_mechs %>% glimpse()

  nrow(df_mechs) == nrow(distinct(df_mechs, mech_code))

  # All Mechs
  df_mechs %>%
    db_create_table(tbl_mechanisms, ., conn,
                    meta = cols_msd_sites,
                    pkeys = "mech_code",
                    overwrite = T)

  # All Partners
  df_mechs %>%
    select(any_of(str_msd_sites$mech_partners)) %>%
    distinct() %>%
    db_create_table(tbl_mechs_partners, ., conn,
                    meta = cols_msd_sites,
                    pkeys = "prime_partner_uei",
                    overwrite = T)

  # All Mechs/Awards
  df_mechs %>%
    select(any_of(str_msd_sites$mech_awards)) %>%
    distinct() %>%
    db_create_table(tbl_mechs_awards, ., conn,
                    meta = cols_msd_sites,
                    pkeys = "mech_code",
                    overwrite = T)

  remove(df_mechs)


  ## Data Elements, Indicators & Disaggs ----
  tbl_elements <- "msd_dataelements"
  tbl_inds <- "msd_indicators"
  tbl_disaggs <- "msd_disaggregates"
  tbl_coc <- "msd_categoryoptioncombos"
  tbl_ages <- "msd_age_bands"

  # Data Elements
  df_elmts <- df_sites %>%
    select(any_of(str_msd_sites$dataelements)) %>%
    distinct() %>%
    rowwise() %>%
    mutate(dataelement = msd_build_elements(
      indicator, numeratordenom, indicatortype, disaggregate)) %>%
    ungroup() %>%
    relocate(dataelement, .before = standardizeddisaggregate)

  df_elmts %>% glimpse()

  df_elmts %>%
    distinct(dataelement, standardizeddisaggregate, categoryoptioncomboname) %>%
    nrow() %>%
    equals(nrow(df_elmts))

  df_elmts %>% find_pkeys()
  df_elmts %>% find_pkeys(colnames = T)

  df_elmts %>%
    db_create_table(tbl_elements, ., conn,
                    meta = cols_msd_sites,
                    pkeys = c("dataelement",
                              "standardizeddisaggregate",
                              "categoryoptioncomboname"),
                    overwrite = T)

  # Indicators
  df_elmts %>%
    select(one_of(str_msd_sites$indicators)) %>%
    distinct() %>%
    add_count(dataelement, standardizeddisaggregate) %>%
    filter(n > 1)

  df_elmts %>%
    select(one_of(str_msd_sites$indicators)) %>%
    distinct() %>% find_pkeys(colnames = T)

  df_elmts %>%
    select(one_of(str_msd_sites$indicators)) %>%
    distinct() %>%
    db_create_table(tbl_inds, ., conn,
                    meta = cols_msd_sites,
                    pkeys = c('dataelement', 'standardizeddisaggregate'),
                    overwrite = T)

  # Disaggregates
  df_elmts %>%
    select(one_of(str_msd_sites$disaggregates)) %>%
    distinct() %>%
    add_count(dataelement, standardizeddisaggregate) %>%
    filter(n > 1)

  df_elmts %>%
    select(one_of(str_msd_sites$disaggregates)) %>%
    distinct() %>%
    find_pkeys(colnames = T)

  df_elmts %>%
    select(one_of(str_msd_sites$disaggregates)) %>%
    distinct() %>%
    mutate_all(~case_when(is.na(.) ~ "", TRUE ~ .)) %>%
    db_create_table(tbl_disaggs, ., conn,
                    meta = cols_msd_sites,
                    pkeys = c("indicator", "numeratordenom", "indicatortype",
                              "disaggregate", "standardizeddisaggregate",
                              "otherdisaggregate", "otherdisaggregate_sub"),
                    overwrite = T)

  # age bands
  df_sites %>%
    select(one_of(str_msd_sites$age_disaggregates)) %>%
    distinct() %>%
    add_count(fiscal_year, indicator, ageasentered) %>%
    filter(n > 1)

  df_sites %>%
    select(one_of(str_msd_sites$age_disaggregates)) %>%
    distinct() %>%
    find_pkeys(colnames = T)

  df_sites %>%
    select(one_of(str_msd_sites$age_disaggregates)) %>%
    distinct() %>%
    mutate(across(c(starts_with("age"), "trendscoarse"),
                  ~case_when(is.na(.) ~ "", TRUE ~ .))) %>%
    arrange(desc(fiscal_year), indicator, ageasentered) %>%
    db_create_table(tbl_ages, ., conn,
                    meta = cols_msd_sites,
                    pkeys = c("fiscal_year", "indicator", "ageasentered"),
                    overwrite = T)

  # COC
  df_sites %>%
    select(one_of(str_msd_sites$categoryoptioncombos)) %>%
    distinct() %>%
    find_pkeys(colnames = T)

  df_sites %>%
    select(one_of(str_msd_sites$categoryoptioncombos)) %>%
    distinct() %>%
    mutate_all(~case_when(is.na(.) ~ "", TRUE ~ .)) %>%
    db_create_table(tbl_coc, ., conn,
                    meta = cols_msd_sites,
                    pkeys = names(.),
                    overwrite = T)


  ## Data -----

  # Table Names
  tbl_sitexim_wide_rsts <- "msd_sitexim_wide_results"
  tbl_sitexim_wide_trgts <- "msd_sitexim_wide_targets"
  tbl_sitexim_long <- "msd_sitexim_long"

  df_elmts <- df_sites %>% msd_dataelements()

  df_elmts %>%
    group_by_all() %>%
    count() %>%
    filter(n > 1)

  # Extract Fact table
  #df_fact <- df_sites %>% msd_fact()
  df_fact <- df_sites %>%
    msd_fact(df_msd = ., df_elmts = df_elmts)

  df_fact %>% glimpse()

  #df_fact %>% find_pkeys(colnames = T)

  # df_fact %>%
  #   filter(is.na(targets)) %>%
  #   find_pkeys(colnames = T)
  #
  # df_fact %>%
  #   filter(!is.na(targets)) %>%
  #   find_pkeys(colnames = T)

  # Results
  df_fact %>%
    filter(is.na(targets)) %>%
    db_create_table(tbl_sitexim_wide_rsts, ., conn,
                    meta = cols_msd_sites,
                    pkeys = c("fiscal_year", "orgunituid", "mech_code",
                              "dataelement", "standardizeddisaggregate",
                              "categoryoptioncomboname"),
                    overwrite = T)

  # Targets
  df_fact %>%
    filter(!is.na(targets)) %>%
    db_create_table(tbl_sitexim_wide_trgts, ., conn,
                    meta = cols_msd_sites,
                    pkeys = c("fiscal_year", "orgunituid", "mech_code",
                              "dataelement", "standardizeddisaggregate",
                              "categoryoptioncomboname"),
                    overwrite = T)

  # Results and Targets
  df_fact %>%
    mutate(across(.cols = where(is_character), ~case_when(is.na(.) ~ "", TRUE ~ .))) %>%
    mutate(across(targets, ~ case_when(is.na(.) ~ 0, TRUE ~ .))) %>%
    db_create_table(tbl_sitexim_wide, ., conn,
                    meta = cols_msd_sites,
                    pkeys = c("fiscal_year", "orgunituid", "mech_code",
                              "dataelement", "standardizeddisaggregate",
                              "categoryoptioncomboname", "targets"),
                    overwrite = T)


  # Reshape Fact table long

  df_fact %>%
    msd_reshape_fact() %>%
    find_pkeys(colnames = T)

  df_fact %>%
    msd_reshape_fact() %>%
    db_create_table(tbl_sitexim_long, ., conn,
                    meta = cols_msd_sites,
                    pkeys = c("fiscal_year", "period", "orgunituid", "mech_code",
                              "dataelement", "standardizeddisaggregate",
                              "categoryoptioncomboname", "value_type"),
                    overwrite = T)

  # Add Foreign Keys
  fkeys <- list("msd_orgunits" = "orgunituid",
                "msd_mechanisms" = "mech_code",
                "msd_dataelements" = c("dataelement", "standardizeddisaggregate", "categoryoptioncomboname"))


  walk(names(fkeys), function(.x) {
    print(.x)
    print(fkeys[[.x]])
    print(base::paste(fkeys[[.x]], collapse = ", "))
  })

  db_update_table(tbl_sitexim_long, conn, fkeys = fkeys)

  # i Adding Foreign Key(s): msd_sitexim_long (orgunituid) => msd_orgunits (orgunituid)
  # <SQL> ALTER TABLE msd_sitexim_long ADD FOREIGN KEY (orgunituid) REFERENCES msd_orgunits (orgunituid);
  # i Adding Foreign Key(s): msd_sitexim_long (mech_code) => msd_mechanisms (mech_code)
  # <SQL> ALTER TABLE msd_sitexim_long ADD FOREIGN KEY (mech_code) REFERENCES msd_mechanisms (mech_code);
  # i Adding Foreign Key(s): msd_sitexim_long (dataelement, standardizeddisaggregate, categoryoptioncomboname) => msd_dataelements (dataelement, standardizeddisaggregate, categoryoptioncomboname)
  # <SQL> ALTER TABLE msd_sitexim_long ADD FOREIGN KEY (dataelement, standardizeddisaggregate, categoryoptioncomboname) REFERENCES msd_dataelements (dataelement, standardizeddisaggregate, categoryoptioncomboname);























  ## Split MSD Datasets
  #df_sitexim <- df_sites %>% msd_split()




