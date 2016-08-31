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
impalaConnectionUrl <- "jdbc:hive2://datanode1dns:port/;auth=noSasl"
conn <- dbConnect(drv, dbname = "default", impalaConnectionUrl)

# Show tables
dbListTables(conn) 
#or 
dbGetQuery(conn, "show tables")

# Get all elements of a table
d <- dbReadTable(conn, "table_name")
#or
d <- dbGetQuery(conn, "select * from table_name")

# Create a table in parquet format
dbSendUpdate(conn, "CREATE TABLE table_name (attribute1 string, attribute2 int) STORED AS PARQUET")

# Insert data in a table
dbGetQuery(conn, "INSERT INTO table_name VALUES ('test', 1)")

