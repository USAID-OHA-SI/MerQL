## Packages

#install.packages("DBI")
#install.packages("odbc")

library(tidyverse)
library(glamr)
library(gophr)

source("Scripts/00_Utilities.R")

## DB Paths
cntry <- "Nigeria"

db_sqlite <- cntry %>% paste0("_msd_sitexim.db")

## Set Postgres DBMS Access
## 1 time set up

set_key(service = pg_service("local"), "host")
set_key(service = pg_service("local"), "port")
set_key(service = pg_service("local"), "database")
set_key(service = pg_service("local"), "username")
set_key(service = pg_service("local"), "password")

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

sort(unique(odbc::odbcListDrivers()[[1]]))

# Test SQLite Connection
# Note: The SQLite DB file will be created if it does not already exist

slite_conn <- db_connection(db_name = db_sqlite)

slite_conn

RSQLite::dbDisconnect(slite_conn)

# Test Postgres Connection
# Note: driver RPostgres::Postgres() #=> works better vs RPostgreSQL::PostgreSQL()

# conn <- db_connection(db_name = "postgres")

conn <- db_connection(db_name = pg_database())

conn

# Get connection info
DBI::dbGetInfo(conn)

str(conn)

# Get db name from connecion instance
db_name(conn)

# List databases
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

## Create Database
sql_cmd_1a <- "
  CREATE DATABASE test WITH
    OWNER = postgres ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;
"

sql_cmd_1b <- "
  CREATE DATABASE $1 WITH
    OWNER = $2 ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;
"

dbExecute(conn, sql_cmd_1a)
dbExecute(conn, sql_cmd_1b, params = list(str_to_lower(cntry), pg_user()))

## Create Schema
sql_cmd_schema_1a <- "
-- SCHEMA: hfr

-- DROP SCHEMA IF EXISTS hfr ;

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
dplyr::tbl(conn, tblname) %>% head() %>% show_query()
dplyr::tbl(conn, tblname) %>% head() %>% explain()

dplyr::tbl(conn, tblname) %>% head()
dplyr::tbl(conn, tblname) %>% head() %>% collect()
dplyr::tbl(conn, tblname) %>% collect()

