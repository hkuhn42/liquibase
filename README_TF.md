Build with maven 

 ./mvnw package -DskipTests
 
 
 copy .../liquibase/liquibase-core/target/liquibase-core-0-SNAPSHOT.jar as liquibase-core.jar into a distros internal/lib
 
 
 create a props file
 
 ```test
url = jdbc:oracle:thin:@localhost:51522/ORCLPDB1
username = trend
password = trend
changeLogFile=changelog/db.changelog-root.yaml
liquibase.command.includeSchema=false
liquibase.command.omitSchemas=true
liquibase.command.splitOutput=true
 ```
 
 run with
  
<path to patched distro>/liquibase-tf-5.0.1/liquibase --defaultsFile=liquibase.properties update-sql --output-file main.sql