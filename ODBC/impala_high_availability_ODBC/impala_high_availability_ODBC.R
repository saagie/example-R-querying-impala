

 ### Functions are built to ensure random node connexion to impala ###
#Load of odbc package
library(odbc)
DATANODES <-  'dn1;dn2;dn3;dn4;dn5;dn6;dn7;dn8;dn9'
DATANODES <- unlist(strsplit(DATANODES,";"))

#Test for working node in the list provided, return a named vector (true/false)
test_datanode <- function(x, user, password, timeout){
  tryCatch(
    expr = {
    before <- getTaskCallbackNames()
    con <-DBI::dbConnect(odbc::odbc(),
                          Driver   = ifelse(.Platform$OS.type == "windows",
                                            "Cloudera ODBC Driver for Impala",
                                            "Cloudera ODBC Driver for Impala 64-bit"),
                          Host     =  x,
                          Port     = 21050,
                          Schema   = "default",
                          AuthMech = 3,
                          UseSASL  = 1,
                          UID      = user,
                          PWD      = password,
                         timeout   = timeout
  )
    after <- getTaskCallbackNames()
    #avoid warnings due to the connections tab from Rstudio
    # before + after + removeTaskCallback can be deleted if used out of Rstudio
    removeTaskCallback(which(!after %in% before))
    return(TRUE)
  },
  error = function(e){
    return(FALSE)
  })
}


# Allow to set a up a random node connexion
random_node_connect <- function(nodelist, user, password, timeout = 0.5){
  if(missing(nodelist)){
    stop("nodelist is mandatory, please provide it.", call. = FALSE)
  }
  if(missing(user) | missing(password)){
    stop("user or passsword is missing, please provide it.", call. = FALSE)
  }
  #Get a vector TRUE/FALSE for responding nodes
  answered <- sapply(DATANODES, test_datanode, user = user, password = password,  timeout = timeout)
  #Get the names of the reponding nodes
  nodes_names <- names(answered[answered == TRUE])
  #Choose a random one :
  rand_node <- nodes_names[sample(1:length(nodes_names), 1)]
  #Message with dn choosen
  message(paste0("Connection to : ", rand_node))
  #return connexion object randomly choosen in the list of available working nodes
  return(DBI::dbConnect(odbc::odbc(),
                        Driver   = ifelse(.Platform$OS.type == "windows",
                                          "Cloudera ODBC Driver for Impala",
                                          "Cloudera ODBC Driver for Impala 64-bit"),
                        Host     =rand_node,
                        Port     = 21050,
                        Schema   = "default",
                        AuthMech = 3,
                        UseSASL  = 1,
                        UID      = user,
                        PWD      = password,
                        timeout   = timeout
  )
  )
}
### Examples of usage ############################################################
#Return a list of available dn
available_dn <- sapply(DATANODES,
                       test_datanode,
                       user= Sys.getenv("MY_USER"),
                       password = Sys.getenv("MY_PWD"),
                       timeout = 0.4)

# Set-up a connexion to a random available dn
con <- random_node_connect(nodelist = DATANODES,
                           user     = Sys.getenv("MY_USER"),
                           password = Sys.getenv("PW_PWD"),
                           timeout = 0.2
                           )

#################################################################################