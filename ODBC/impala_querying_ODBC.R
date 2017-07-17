library(DBI) # Standard database interface
library(odbc) # Connect to ODBC databases using DBI
library(dplyr) # Data manipulation library
library(dbplyr) # Converts data manipulation written in R to SQL
library(implyr) # Same as above but specific for Impala

# Windows :
# Dl msi from https://www.cloudera.com/downloads/connectors/impala/odbc/2-5-39.html
# install .msi

# Linux
# Dl deb from https://www.cloudera.com/downloads/connectors/impala/odbc/2-5-39.html
# install .deb
# sudo odbcinst -i -d -f /opt/cloudera/impalaodbc/Setup/odbcinst.ini

# Check that the driver is correctly installed, then copy and paste the driver name
# in the Driver attribute of the dbConnect function
odbcListDrivers()
# Default name on Windows is usually "Cloudera ODBC Driver for Impala"
# Default name on Linux is usually "Cloudera ODBC Driver for Impala 64-bit"

con <- DBI::dbConnect(odbc::odbc(),
                      Driver   = "Cloudera ODBC Driver for Impala 64-bit",
                      Host     = "host",
                      Port     = 21050,
                      Schema   = "default",
                      # Remove the options below if you don't need authentification
                      AuthMech = 3,
                      UseSASL  = 1,
                      UID      = "user",
                      PWD      = "password")

# On the version 2.5.39 of the Linux Impala ODBC driver, the method dbListTables returns only the first letter of each schema and table
# One workaround is to use plain SQL "show schemas" and "show tables"

# List all tables from all schemas
dbListTables(con)
dbGetQuery(con, 'show schemas')
dbGetQuery(con, 'show tables')

sample <- DBI::dbReadTable(con, "sample") # Fetch all data from a table

# Execute sql statement on a connection
dbGetQuery(con, 'select count(*) from sample')
forecast <- dbGetQuery(con, 'select * from forecast.forecast_weekly limit 200')

# Create a lazy tbl from an Impala table
forecast_db <- tbl(con, in_schema('forecast', 'forecast_weekly'))
forecast_db # Print columns and column types

# The query is written in dplyr syntax, but executed on a remote sql database
query <- forecast_db %>% 
    summarise(mean_forecast = forecast %>% as.numeric() %>% mean)

show_query(query) # Show the query that will be executed
query # Executes the lazy query when the result is printed

# Example of usable dplyr verbs
forecast_db %>% 
    filter(prediction_date == max(prediction_date)) %>%
    group_by(reference) %>%
    summarise(forecast_mean = mean(forecast), 
              forecast_max = max(forecast), 
              nb_forecast_total = n())

sales_db <- tbl(con, in_schema('forecast', 'sales_weekly'))
sales_db

forecast_db <- forecast_db %>%
    mutate(reference = as.character(reference))

diff_db <- inner_join(forecast_db, sales_db, by=c('reference', 'year', 'week'))

diff_by_ref <- diff_db %>% 
    group_by(reference, year, week) %>% 
    summarise(diff_by_week = abs(forecast - quantity)) %>% # Difference between forecast and reality for each prediction
    group_by(reference) %>%
    summarise(diff_by_ref = sum(diff_by_week)) # Sum of all differences for each reference

diff_db # Executes all the lazy queries above 
    
