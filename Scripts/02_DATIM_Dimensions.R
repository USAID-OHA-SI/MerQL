library(tidyverse)
library(glamr)

f_nameable <- c('id', 'name', 'shortName', 'code', 'description', 'created', 'lastUpdated')
f_identifiable <- c('id', 'name', 'code', 'created', 'lastUpdated')

# Ideal use cases - query datim analytics

dims <- datim_dimensions()

# Type of organisational unit

datim_dim_items(dimension = "Type of organisational unit")

# Planning Prioritization Set

datim_dim_items(dimension = "Planning Prioritization Set")


# Disaggregation Type

datim_dimension(name = "Disaggregation Type")

datim_dim_items(dimension = "Disaggregation Type")

datim_dim_items(dimension = "Disaggregation Type", fields = "*")
datim_dim_items(dimension = "Disaggregation Type")

# Funding Agency

datim_dim_items(dimension = "Funding Agency")

# Funding Mechanism

datim_dim_items(dimension = "Funding Mechanism")

datim_dim_items(dimension = "Funding Mechanism", fields = "*")

datim_dim_items(dimension = "Funding Mechanism",
                fields = c("id", "code", "displayName", "shortName")) %>%
  rename(name = item) %>%
  separate(short_name,
           into = c("iso3", "mech_code", "mech_name"),
           sep = " - ") %>%
  relocate(code, name, .after = id)

# Partner Location

datim_dim_items(dimension = "Partner Location")



# Modalities
datim_dim_items(dimension = "HTS Modality (USE ONLY for FY22 Results/FY23 Targets)")

dims %>%
  filter(str_detect(dimension, "HTS Modality")) %>%
  pull(dimension) %>%
  map_dfr(~datim_dim_items(dimension = .x)) %>%
  datim_clean_modalities() %>%
  pivot_wider(id_cols = c(sitetype, name),
              names_from = fiscal_year,
              values_from = short_name)

dim_hts_mods <- datim_dim_items(
  dimension = "HTS Modality (USE ONLY for FY22 Results/FY23 Targets)")

dim_hts_mods %>% datim_clean_modalities()

# Technical Area

datim_dim_items(dimension = "Technical Area")

datim_dim_items(dimension = "Technical Area", fields = "*")

datim_dim_items(dimension = "Technical Area",
                fields = c('id', 'name', 'shortName', 'displayName',
                           'aggregationType', 'dimensionItemType'))

datim_dim_items(dimension = "Technical Area", fields = ":nameable")
datim_dim_items(dimension = "Technical Area", fields = ":identifiable")

# Data Elements

datim_dim_items(dimension = "Technical Area")