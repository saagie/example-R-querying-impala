library(DBI) # Standard database interface
library(odbc) # Connect to ODBC databases using DBI
library(dplyr) # Data manipulation library
library(dbplyr) # Converts data manipulation written in R to SQL
library(implyr) # Same as above but specific for Impala

########## Connection without Kerberos ##########

con <- DBI::dbConnect(odbc::odbc(),
                      Driver   = "Cloudera ODBC Driver for Impala 64-bit",
                      Host     = "dn1.pX.company.prod.saagie.io",
                      Port     = 21050,
                      Schema   = "default",
                      AuthMech = 3,
                      UseSASL  = 1,
                      UID      = "user",
                      PWD      = "password")


########## Connection with Kerberos ##########

library(getPass)
# Method 1 (interactive) : Use in Rstudio. Interactive pop up to enter password
system('kinit user',input=getPass('Enter your password: '))

# Method 2 (scripts) : Use outside of Rstudio. 
# Password is written in command line or stored in a environment variable
# Uncomment next line to use
# system('echo password | kinit user')

con <- dbConnect(odbc::odbc(),
                 Driver   = "Cloudera ODBC Driver for Impala 64-bit",
                 Host     = "dn1.pX.company.prod.saagie.io",
                 Port     = 21050,
                 Schema   = "default",
                 AuthMech = 1)


########## Execute queries on connection ##########

# List all tables from all schemas
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
    
