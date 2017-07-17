# Load/Download rjdbc library
if (!require("RJDBC")) {
  install.packages("RJDBC", repos = "http://cran.r-project.org", dependencies = T)
  library("RJDBC")
}

# Load jdbc_driver
drv <- JDBC(driverClass = "org.apache.hive.jdbc.HiveDriver",
            classPath = list.files("impala_jdbc_folder_path",pattern="jar$",full.names=T),
            identifier.quote="`")

# Connection to impala on the default database
impalaConnectionUrl <- "jdbc:hive2://IP:Port/;auth=noSasl"
conn <- dbConnect(drv, impalaConnectionUrl)

# Show tables (all databases)
dbListTables(conn) 
# Show tables (the database "default")
dbGetQuery(conn, "show tables")

# Get all elements of a table (in database "default")
dbReadTable(conn,"table_name")
# OR
dbGetQuery(conn, "SELECT * FROM table_name")
# Get all elements of a table (other that the database "default")
dbReadTable(conn,"nameBDD.table_name")

# Create a table in parquet format
dbSendUpdate(conn, "CREATE TABLE table_name (attribute1 string, attribute2 int) STORED AS PARQUET")

# Insert data in a table
dbGetQuery(conn, "INSERT INTO table_name VALUES ('test', 1)")

# Refresh Tables
dbGetQuery(conn, "INVALIDATE METADATA")
