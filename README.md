# About 

The goal is to write a parser in Java that parses web server access log file, loads the log to MySQL and checks if a given IP makes more than a certain number of requests for the given duration. 

## Building

Make sure you have Maven installed or use the mvnw script to install it for you

`mvn clean package` or `mvnw clean package`

To run a local instance you use either `mvn spring-boot:run` or `mvnw spring-boot:run`


## Usage

`java -jar ./target/org.eu.mmacedo.mysql.log.sink-0.0.1-SNAPSHOT.jar --help`
```text
usage: Mysql.Log.Sink
    --accesslog <arg>                    Access file to process
 -c,--clear                              empty database before processing
    --duration <arg>                     Duration interval (hourly or
                                         daily)
 -h,--help                               show help.
 -q,--query                              no processing sink, just query
    --spring.datasource.url <arg>        database url
    --spring.output.ansi.enabled <arg>
    --startDate <arg>                    Start Date in
    --threshold <arg>                    Threshold limit (integer >0)
```
### Example

`java -jar ./target/org.eu.mmacedo.mysql.log.sink-0.0.1-SNAPSHOT.jar --startDate=2017-01-01.15:00:00 --duration=hourly --threshold=200 --clear --accesslog="access.log" --spring.datasource.url="jdbc:mysql://localhost:3306/log?user=root&password=abc123&useServerPrepStmts=false&rewriteBatchedStatements=true&sessionVariables=@@global.general_log=OFF,bulk_insert_buffer_size=16777216,SQL_LOG_BIN=0&useSSL=false"`

## Sql

Sqls are provided inside `/src/main/resources/application.properties` and `/src/main/resources/db/migration`

## Privileges

Should have root privileges for bulk optimizations

## Defaults

Sensible defaults are provide inside `/src/main/resources/application.properties` including database url and others