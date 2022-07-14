##  PROJECT: PEPFAR/Datim Data Management
##  AUTHOR:  Baboyma Kagniniwa | USAID
##  PURPOSE: Extract Datim Data Exchange Resources
##  LICENCE: MIT
##  DATE:    2021-10-08
##  UPDATED: 2022-07-14

# Libraries ----

  library(tidyverse)
  library(gophr)
  library(glamr)
  library(janitor)
  library(DBI)
  library(uuid)

  source("./Scripts/00_Utilities.R")

# SQL Views

# DATIM Data Exchange ----

# Org units ----

# OU Countries
df_ous_view <- datim_sqlviews(view_name = "OU countries", dataset = TRUE)

# Org units
# Facility / Site / Community / PSNU / SNU / Country / OU
# Query var=OU:ISO3

ou = "Nigeria"

cntry_code <- df_ous_view %>%
  filter(orgunit_name == ou) %>%
  pull(orgunit_code)

df_orgs_view <- datim_sqlviews(
  view_name = "Data Exchange: Organisation Units",
  dataset = TRUE,
  query = list("OU" = cntry_code)
)

df_orgs_view <- datim_orgview(cntry_code)

# All OU / Orgunits
df_ous_view %>%
  pmap_dfr(~datim_sqlviews(
    view_name = "Data Exchange: Organisation Units",
    dataset = TRUE,
    query = list("OU" = ..3)
  ))

# Mechanisms ----

# Mechanisms - AOC = Attribute Option Combo
df_mechs_view <- datim_sqlviews(
  view_name = "Mechanisms partners agencies OUS Start End",
  dataset = TRUE)

df_mechs_view %>%
  rename(
    mech_code = code,
    operatingunit = ou,
    prime_partner = partner,
    prime_partner_id = primeid,
    funding_agency = agency
  ) %>%
  mutate(
    mech_name = str_remove(mechanism, mech_code),
    award_number = str_extract(mech_name, "(?<=-[:space:]).*(?=[:space:]-)"),
    mech_name = str_trim(mech_name),
    mech_name = str_remove(mech_name, "-[:space:].*[:space:]-[:space:]|-[:space:]")
  ) %>%
  select(uid, mech_code, mech_name, mechanism, everything())

df_mechs_view <- datim_mechview()

# Partners
df_partners_view <- datim_sqlviews(
  view_name = "Country, Partner, Agencies",
  dataset = TRUE)

df_partners_view %>%
  rename(
    operatingunit = ou,
    prime_partner = partner,
    funding_agencies = agencies
  )

df_partners_view <- datim_ppview()

# Data Elements ----

# DataSets

# All datasets - Used for different type of PEPFAR Data
df_datasets <- datim_sqlviews(view_name = "Data sets", dataset = TRUE)

# Filter for Datim Exchange Data Elements only
df_datasets <- df_datasets %>%
  filter((str_detect(name, "^MER Results") |
         str_detect(name, "^Host Country Results") |
         str_detect(name, "^Planning Attributes") |
         str_detect(name, "^MER Targets") |
         str_detect(name, "^MER Target Setting") |
         str_detect(name, "^Host Country Targets") |
         str_detect(name, "^Host Country Targets"))
         & str_detect(name, ".*FY.*", negate = T))

# Parse out different components
df_datasets <- df_datasets %>%
  separate(name, into = c("source", "name"), sep = ": ") %>%
  mutate(
    data_type = case_when(
      str_detect(source, " Targets$|.*Target.*") ~ "Targets",
      TRUE ~ "Results"),
    period = str_extract(name, "(?<=FY).*"),
    period = case_when(
     !is.na(period) ~ paste0("FY", period),
     TRUE ~ period)) %>%
  filter(is.na(period))

# Data Elements

# Ref URL: https://datim.zendesk.com/hc/en-us/articles/115002334246-DATIM-Data-Import-and-Exchange-Resources#current
# Query var=dataSets:uids
#
# MER Results: Facility Based
datim_sqlviews(view_name = "Data sets, elements and combos paramaterized",
               dataset = TRUE,
               query = list("dataSets" = "BHlhyPmRTUY"))

datim_sqlviews(view_name = "Data sets, elements and combos paramaterized section forms",
               dataset = TRUE,
               query = list("dataSets" = "BHlhyPmRTUY"))

# Batch / Query all Data Elements
df_datasets %>%
  pmap_dfr(~datim_sqlviews(
             view_name = "Data sets, elements and combos paramaterized",
             dataset = TRUE,
             query = list("dataSets" = ..4)))

df_datasets %>%
  pmap_dfr(possibly(~datim_deview(datasetuid = ..4), otherwise = NULL))

df_datasets %>%
  pull(uid) %>%
  map_dfr(possibly(~datim_deview(datasetuid = .x), otherwise = NULL))

# Periods ----
#
# Periods - Days (yyyyMMdd), Quarters (yyyyQn), Financial Year (yyyyOct)
df_periods <- datim_sqlviews(view_name = "Period information", dataset = TRUE)

df_periods <- datim_peview()
