## Packages

#install.packages("DBI")
#install.packages("odbc")

library(tidyverse)

source("Scripts/00_Utilities.R")

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

## Establish Connection with DB Server

sort(unique(odbc::odbcListDrivers()[[1]]))

RPostgres::Postgres()
RPostgreSQL::PostgreSQL()

# conn <- db_connection()
# conn <- db_connection(db_name = "dvdrental")
# conn <- db_connection(db_name = "dev")

DBI::dbDisconnect(conn)

conn

DBI::dbGetInfo(conn)

str(conn)

db_name(conn)

db_list(conn)

db_schemas(conn)

dbListTables(conn)

db_tables(conn)
db_tables(conn, schema = "public")
db_tables(conn, schema = "public", details = T)
db_tables(conn, schema = "pg_toast", details = T)

## Create Database
sql_cmd_1a <- "
  CREATE DATABASE dev1 WITH
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
dbExecute(conn, sql_cmd_1b, params = list("dev2", pg_user()))

## Create table from data from
data("iris")

dbCreateTable(conn, "tbl_iris", tibble::as_tibble(iris))
dbAppendTable(conn, "tbl_iris", tibble::as_tibble(iris))

dplyr::tbl(conn, "tbl_iris") %>% head() %>% show_query()
dplyr::tbl(conn, "tbl_iris") %>% head() %>% explain()
dplyr::tbl(conn, "tbl_iris") %>% head()
dplyr::tbl(conn, "tbl_iris") %>% head() %>% collect()
