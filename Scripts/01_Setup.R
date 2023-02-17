## Packages

#install.packages("DBI")
#install.packages("odbc")

library(tidyverse)
library(glamr)
library(gophr)
library(DBI)

source("Scripts/00_Utilities.R")

# Params

dir_data <- "./Data"

cntry <- "Nigeria"

msd_type <- "sitexim"

## SQLite DB Path

db_sqlite <- cntry %>%
  tolower() %>%
  paste0("_msd_", msd_type, ".db")

## Set Postgres DBMS Access - 1 time set up

# set_key(service = pg_service("local"), "host")
# set_key(service = pg_service("local"), "port")
# set_key(service = pg_service("local"), "database")
# set_key(service = pg_service("local"), "username")
# set_key(service = pg_service("local"), "password")

## Test Postgres DBMS Access

get_key("local-postgres", "host")
get_key(pg_service("local"), "host")
pg_host()

get_key("local-postgres", "port")
pg_port()

get_key("local-postgres", "database")
pg_database()

get_key("local-postgres", "username")
pg_user()

get_key("local-postgres", "password")
pg_pwd()

## List odbc DB Connection drivers

db_drivers <- sort(unique(odbc::odbcListDrivers()$name))

db_drivers[which(stringr::str_detect(tolower(db_drivers), "postgres"))]

# Test SQLite Connection
# Note: The SQLite DB file will be created if it does not already exist

slite_conn <- db_sqlite %>%
  file.path(dir_data, .) %>%
  db_connection(db_name = .)

slite_conn

RSQLite::dbDisconnect(slite_conn)

slite_conn

# Test Postgres Connection
# Note: driver RPostgres::Postgres() #=> works better vs RPostgreSQL::PostgreSQL()

conn <- db_connection(db_name = pg_database())

# Get detailled connection info
DBI::dbGetInfo(conn)

# Get db name from the connecion instance
db_name(conn)

# Get the list databases
db_list(conn)

# List schemas
# Note: only schemas with tables will be listed
db_schemas(conn)

# List tables
DBI::dbListTables(conn)

db_tables(conn)
db_tables(conn, details = T)
db_tables(conn, schema = "public")
db_tables(conn, schema = "public", details = T)
db_tables(conn, schema = "datim", details = T)

db_tables(conn, schema = "test", details = T)

## Create Database with SQL Statement
sql_cmd_1a <- "CREATE DATABASE test123;"

DBI::dbExecute(conn, sql_cmd_1a)

db_list(conn)

## Create Database with Parametric SQL Statement
sql_cmd_1b <- "
  CREATE DATABASE $1 WITH
    OWNER = $2;
"

sql_cmd_1c <- "
  CREATE DATABASE ? WITH
    OWNER = ?;
"

sql_cmd_1d <- "
  CREATE DATABASE ?name WITH
    OWNER = ?owner;
"

DBI::sqlInterpolate(ANSI(), sql_cmd_1c, "msd", pg_user())

DBI::sqlInterpolate(conn, sql_cmd_1d,
                    .dots = list(name = "msd", owner = pg_user()))

DBI::dbExecute(conn, sql_cmd_1b, params = list("msd", pg_user()))

DBI::dbExecute(conn, sql_cmd_1c, params = list("msd", pg_user()))
DBI::dbExecute(conn,
               DBI::sqlInterpolate(ANSI(), sql_cmd_1c, "msd", pg_user()))

db_new <- "sims"
db_owner <- pg_user()
db_stmt <- glue::glue_sql(
  "CREATE DATABASE {`db_new`} WITH OWNER = {`db_owner`};",
  .con = conn)

DBI::dbExecute(conn, db_stmt)

#DBI::dbExecute(conn, sql_cmd_1d, params = list(name = "msd", owner = pg_user()))

#DBI::dbExecute(conn, sql_cmd_1b, params = list(str_to_lower(cntry), pg_user()))

db_create(name = "fsd", conn)
db_create(name = "hrhd", conn, pg_user())

## Create Schema
sql_cmd_schema_1a <- "
-- SCHEMA: hfr

-- DROP SCHEMA IF EXISTS hfr;

CREATE SCHEMA IF NOT EXISTS hfr
    AUTHORIZATION postgres;

COMMENT ON SCHEMA hfr
    IS 'HFR Schema';

GRANT ALL ON SCHEMA hfr TO postgres;
"

## Add Constraints to table - Primary Keys, Foreign Keys

sql_cmd_2a <- "
  ALTER TABLE $1
  ADD PRIMARY KEY ($2);
"

sql_cmd_2b <- "
  ALTER TABLE test_fact
  ADD CONSTRAINT test_fkey
  FOREIGN KEY (id)
  REFERENCES test_dim1 (id)
"

sql_cmd_2c <- "
  ALTER TABLE $1
  ADD CONSTRAINT $2
  FOREIGN KEY ($3)
  REFERENCES $4 ($5)
"

## Create sample table from iris data

data("iris")

tblname <- "test_iris"

# Table will be created based on df columns / data type
DBI::dbCreateTable(conn, tblname, tibble::as_tibble(iris))

# Load data into table
DBI::dbAppendTable(conn, tblname, tibble::as_tibble(iris))

# Test Access / Query with dplyr
# Note: this creates a table from the data source [Postgres DB Table]
dplyr::tbl(conn, tblname)

dplyr::tbl(conn, tblname) %>%
  head() %>%
  show_query()

dplyr::tbl(conn, tblname) %>%
  filter(Sepal.Width > 2, Sepal.Length == 5) %>%
  show_query()

dplyr::tbl(conn, tblname) %>% head() %>% explain()

dplyr::tbl(conn, tblname) %>% head(100)

dplyr::tbl(conn, tblname) %>% head(20) %>% collect()

dplyr::tbl(conn, tblname) %>% collect()

