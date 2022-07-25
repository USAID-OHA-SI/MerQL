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

# GLOBALS ----

  # DB Connection
  conn <- db_connection(db_name = pg_database())

  # Schema
  schema <- "datim"

  # Add if does not exists
  db_schema(conn, name = "cir")

# SQL Views ----

  datim_sqlviews()

# DATIM Data Exchange ----

# Org units ----

  # Metadata - OU Org Hierarchy with UIDs
  df_outable <- glamr::get_outable(
    username = datim_user(),
    password = datim_pwd()) %>%
    pivot_longer(cols = ends_with("_lvl"),
                 names_to = "label",
                 values_to = "level") %>%
    mutate(label = str_remove(label, "_lvl"),
           id = uuid::UUIDgenerate(n = nrow(.), output = "uuid"),
           id = as.character(id)) %>%
    relocate(id, .before = 1)

  db_create_table(tbl_name = "datim.ou_levels",
                  fields = df_outable,
                  conn = conn,
                  pkeys = "id",
                  overwrite = T)

  # OU Countries
  df_ous_view <- datim_sqlviews(
      view_name = "OU countries",
      dataset = TRUE) %>%
    relocate(orgunit_uid, .before = 1)

  db_create_table(tbl_name = "datim.ou_countries",
                  fields = df_ous_view,
                  conn = conn,
                  pkeys = "orgunit_uid",
                  overwrite = T)

  # Org units
  # Facility / Site / Community / PSNU / SNU / Country / OU
  # Query var=OU:ISO3

  ou = "Nigeria"

  cntry_code <- df_ous_view %>%
    filter(orgunit_name == ou) %>%
    pull(orgunit_code)

  datim_sqlviews(
    view_name = "Data Exchange: Organisation Units",
    dataset = TRUE,
    query = list("OU" = cntry_code)
  )

  datim_orgview(cntry_code)

  # All OU / Orgunits
  df_orgs_view <- df_ous_view %>%
    pmap_dfr(~datim_sqlviews(
      view_name = "Data Exchange: Organisation Units",
      dataset = TRUE,
      query = list("OU" = ..4)
    ))

  df_orgs_view %>% glimpse()
  df_orgs_view %>% head()

  # Note: orgunit_internal_id should be unique but `Asia Regional Program` & `Asia Program` share the same uid
  df_orgs_view <- df_orgs_view %>%
    mutate(id = uuid::UUIDgenerate(n = nrow(.), output = "uuid"),
           id = as.character(id)) %>%
    relocate(id, .before = 1)

  db_create_table(tbl_name = "datim.organisation_units",
                  fields = df_orgs_view,
                  conn = conn,
                  pkeys = "id",
                  overwrite = T)

  # Mechanisms ----

  # Mechanisms - AOC = Attribute Option Combo
  datim_sqlviews(
    view_name = "Mechanisms partners agencies OUS Start End",
    dataset = TRUE)

  df_mechs_view <- datim_mechview()

  db_create_table(tbl_name = "datim.mechanisms",
                  fields = df_mechs_view,
                  conn = conn,
                  pkeys = "mech_code",
                  overwrite = T)


  # Partners
  datim_sqlviews(
    view_name = "Country, Partner, Agencies",
    dataset = TRUE)

  df_partners_view <- datim_ppview()

  df_partners_view <- df_partners_view %>%
    mutate(id = uuid::UUIDgenerate(n = nrow(.), output = "uuid"),
           id = as.character(id)) %>%
    relocate(id, .before = 1)

  db_create_table(tbl_name = "datim.ou_partners",
                  fields = df_partners_view,
                  conn = conn,
                  pkeys = "id",
                  overwrite = T)

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

  df_de_coc <- df_datasets %>%
    pull(uid) %>%
    map_dfr(possibly(~datim_deview(datasetuid = .x), otherwise = NULL))

  db_create_table(tbl_name = "datim.data_elements_combos",
                  fields = df_de_coc,
                  conn = conn,
                  pkeys = c("dataset", "dataelementuid", "categoryoptioncombouid"),
                  overwrite = T)

  df_de_coc %>% glimpse()

  df_de_coc %>%
    select(!matches(".*optioncomb.*")) %>%
    distinct() %>%
    db_create_table(tbl_name = "datim.data_elements",
                    fields = .,
                    conn = conn,
                    pkeys = c("dataset", "dataelementuid"),
                    overwrite = T)

  df_de_coc %>%
    select(dataset, source, category, type, matches(".*optioncomb.*")) %>%
    distinct() %>%
    db_create_table(tbl_name = "datim.data_coc",
                    fields = .,
                    conn = conn,
                    pkeys = c("dataset", "categoryoptioncombouid"),
                    overwrite = T)

  # Periods ----
  # Periods - Days (yyyyMMdd), Quarters (yyyyQn), Financial Year (yyyyOct)
  datim_sqlviews(view_name = "Period information", dataset = TRUE)

  df_periods <- datim_peview()

  db_create_table(tbl_name = "datim.periods",
                  fields = df_periods,
                  conn = conn,
                  pkeys = "period_id",
                  overwrite = T)


