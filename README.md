Query Impala in R 
=================

2 options are available, ODBC or JDBC.

## JDBC

- Uses Java to connect to Impala
- Requires an installed JVM
- The JVM must have enough RAM to retrive data from the database (if you want to fetch a 2GB table from Impala, you'll need 2 GB in the JVM + 2GB in your R session)
- Very basic integration


## ODBC

- Uses proprietary drivers to connect to Impala
- Direct connection
- Better integration thanks to odbc and implyr packages
