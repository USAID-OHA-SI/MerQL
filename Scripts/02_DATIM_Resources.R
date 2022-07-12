## Libraries
library(tidyverse)
library(janitor)
library(glamr)

source("./Scripts/00_Utilities.R")

## QUERIES

ress <- datim_resources()

# ORGS
datim_resources(res_name = "Organisation Units")
datim_resources(res_name = "Organisation Units", dataset = TRUE)
datim_resources("*", res_name = "Organisation Units", dataset = TRUE)
datim_resources(res_name = "Organisation Unit Levels", dataset = TRUE)
datim_resources(res_name = "Organisation Unit Groups", dataset = TRUE)
datim_resources(res_name = "Organisation Unit Group Sets", dataset = TRUE)

# Data Elements ----

datim_resources(res_name = "Data Elements")
datim_resources(res_name = "Data Elements", dataset = TRUE)

df_res_data_elements <- datim_resources(res_name = "Data Elements", dataset = TRUE)
#df_res_data_elements <- datim_resources("*", res_name = "Data Elements", dataset = TRUE)

df_res_data_elements %>% glimpse()

df_res_data_elements %>%
  filter(!is.na(code)) %>%
  select(id, code, name, shortName, description, created, lastUpdated) %>%
  clean_names() %>%
  mutate(
    data_element_type = case_when(
      str_detect(short_name, ".*TARGET$") ~ "targets",
      TRUE ~ "results"
    ),
    indicator = str_extract(short_name, ".*(?=[:space:]\\()"),
    disaggs = extract_text(short_name, "()")
  ) %>%
  separate(col = disaggs,
           into = c("numerator_denom", "indicator_type", "disaggregate"),
           sep = ", ", remove = F) %>%
  mutate(standardizeddisaggregate = disaggregate,
         modality = case_when(
           str_detect(indicator, "HTS_.*") ~ str_extract(disaggregate, ".*(?=\\/)")
         )) %>%
  relocate(description, created, last_updated, .after = last_col())


df_res_data_elements %>%
  select(id, dimensionItem, dimensionItemType,
         code, name, shortName, displayName, description,
         dataSetElements, dataElementGroups, created, lastUpdated) %>%
  unnest(cols = dataSetElements)

datim_resources("*", res_name = "Data Element Groups", dataset = TRUE)
datim_resources("*", res_name = "Data Element Group Sets", dataset = TRUE)
datim_resources("*", res_name = "Data Element Operands", dataset = TRUE)
datim_resources("*", res_name = "Data Sets", dataset = TRUE)
datim_resources("*", res_name = "Data Stores", dataset = TRUE)

# Indicators ----
datim_resources("*", res_name = "Indicators", dataset = TRUE)
datim_resources("*", res_name = "Indicator Groups", dataset = TRUE)
datim_resources("*", res_name = "Indicator Group Sets", dataset = TRUE)

# Category Combos
datim_resources(res_name = "Category Option Combos", dataset = TRUE)
datim_resources("*", res_name = "Category Option Combos", dataset = TRUE)

datim_resources(res_name = "Categories", dataset = TRUE)
datim_resources(res_name = "Category Combos", dataset = TRUE)
datim_resources("*", res_name = "Category Combos", dataset = TRUE)

datim_resources("*", base_url = b_url, res_name = "Program Data Elements", dataset = TRUE)
datim_resources(base_url = b_url, res_name = "Program Indicators", dataset = TRUE)