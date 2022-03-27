library(magrittr)

#' @title Set a key
#'
#' @param service Name of the service
#' @param name    Name of the key
#'
set_key <- function(service, name) {

  msg <- glue::glue("Please enter value for {service}/{name} key:")

  value <- rstudioapi::askForPassword(prompt = msg)
  value <- stringr::str_trim(value, side = "both")

  if (base::nchar(value) == 0)
    base::stop("ERROR - Invalid value entered")

  keyring::key_set_with_value(service = service,
                              username = name,
                              password = value)
}


#' @title Get key value
#'
#' @param service Name of the service
#' @param name    Name of the key
#'
get_key <- function(service, name) {
  keyring::key_get(service, name)
}


#' @title Postgres Environment
#'
#' @param env Name of environment service
#'
pg_service <- function(env = "local") {
  base::paste0(env, "-postgres")
}


#' @title Postgres Database's host
#'
#' @param env Name of the environment, default set to local
#'
pg_host <- function(env = "local") {
  svc <- pg_service(env)
  get_key(service = svc, name = "host")
}


#' @title Postgres Database's port
#'
pg_port <- function(env = "local") {
  svc <- pg_service(env)
  get_key(service = svc, name = "port")
}


#' @title Postgres Database's name
#'
pg_database <- function(env = "local") {
  svc <- pg_service(env)
  get_key(service = svc, name = "database")
}


#' @title Postgres user's name
#'
pg_user <- function(env = "local") {
  svc <- pg_service(env)
  get_key(service = svc, name = "username")
}


#' @title Postgres user's password
#'
pg_pwd <- function(env = "local") {
  svc <- pg_service(env)
  get_key(service = svc, name = "password")
}

#' @title Postgres Connection
#'
#' @param db_host
#' @param db_port
#' @param db_database
#' @param db_user
#' @param db_pwd
#'
pg_connection <- function(db_host = pg_host(),
                          db_port = pg_port(),
                          db_name = pg_database(),
                          db_user = pg_user(),
                          db_pwd = pg_pwd()) {

  DBI::dbConnect(drv = RPostgres::Postgres(),
                 host = db_host,
                 port = db_port,
                 dbname = db_name,
                 user = db_user,
                 password = db_pwd)

}


#' @title Establish a connection
#'
#' @param db_host
#' @param db_port
#' @param db_database
#' @param db_user
#' @param db_pwd
#' @param db_file
#'
db_connection <- function(db_host = pg_host(),
                          db_port = pg_port(),
                          db_name = pg_database(),
                          db_user = pg_user(),
                          db_pwd = pg_pwd(),
                          db_file = NULL) {

  conn <- NULL

  if (!base::is.null(db_file)) {
    conn <- dbConnect(RSQLite::SQLite(), db_file)
  } else {
    conn <- pg_connection(db_host, db_port, db_name, db_user, db_pwd)
  }

  return(conn)
}


#' @title Retrieve database name from a connection object
#'
#' @param conn DBI::dbConnect instance
#'
db_name <- function(conn) {

  conn_info <- DBI::dbGetInfo(conn)

  name <- conn_info$dbname

  return(name)
}


#' @title Connect to database
#'
#' @description TODO - Switch to a new database
#'
db_connect <- function(conn, db_name = NULL) {

}


#' @title List of Databases
#'
db_list <- function(conn) {

  sql_cmd <- '
    SELECT * FROM pg_database
    WHERE datallowconn = true
    AND datistemplate = false;
  '

  query <- DBI::dbGetQuery(conn, sql_cmd)

  query %>%
    tibble::as_tibble() %>%
    dplyr::pull(datname) %>%
    base::sort()
}


#' @title List database schemas
#'
db_schemas <- function(conn) {

  conn_info <- DBI::dbGetInfo(conn)

  db_name <- conn_info$dbname

  print(paste0("DB = ", db_name))

  sql_cmd <- "
    SELECT DISTINCT table_catalog, table_schema
    FROM information_schema.tables
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    AND table_catalog = $1
    ORDER BY table_schema;
  "

  query <- DBI::dbGetQuery(conn, sql_cmd, params = base::list(db_name))

  schemas <- query %>%
    tibble::as_tibble() %>%
    dplyr::pull(table_schema) %>%
    base::sort()

  return(schemas)
}


#' @title List database and schema tables
#'
db_tables <- function(conn,
                      schema = NULL,
                      details = FALSE) {

  conn_info <- DBI::dbGetInfo(conn)

  db_name <- conn_info$dbname

  print(paste0("DB = ", db_name, ", SCHEMA = ",
               base::ifelse(base::is.null(schema), "All", schema)))

  sql_cmd <- "
    SELECT DISTINCT table_schema, table_name, table_type
    FROM information_schema.tables
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    AND table_catalog = $1
    ORDER BY table_name;
  "

  conn_info <- DBI::dbGetInfo(conn)

  db_name <- conn_info$dbname

  query <- DBI::dbGetQuery(conn, sql_cmd, params = list(db_name))

  tbls <- query %>% tibble::as_tibble()

  if (!base::is.null(schema)) {
    tbls <- tbls %>% dplyr::filter(table_schema == schema)
  }

  if (details) {
    return(tbls)
  }

  tbls <- tbls %>% dplyr::pull(table_name)

  return(tbls)
}


#' @title
db_define <- function() {}

#' @title
db_munge <- function() {}

#' @title
db_query <- function() {}

#' @title
db_control <- function() {}


# ----
# df return - dbFetch is executed to retrieve data
#
# DQL - Send and execute Query, retrieve results
#dbGetQuery()

# DQL - Send and execute Query, no results retrieved
#dbSendQuery()

# DML - Execute an update statement, query n of rows affected
#dbExecute()

# DML - Execute a data manipulation statement
#dbSendStatement()

# DCL - GRANT & REVOKE

# TCL - COMMIT, SAVEPOINT, ROLLBACK, SET transaction, SET constraint
