Query Impala in R with ODBC
=================

## Dependencies

- DBI -> Standard database interface
- odbc -> Connect to ODBC databases using DBI
- dplyr -> Data manipulation library
- dbplyr -> Converts data manipulation written in R to SQL
- implyr -> Same as above but specific for Impala


## Usage

- Install ODBC Drivers (installation details in code)
- Use odbcListDrivers() to get the name of your installed driver
- Fill connection details
- Connect to database
