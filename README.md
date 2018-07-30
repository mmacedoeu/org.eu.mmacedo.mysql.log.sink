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
 -accesslog,--accesslog <arg>               Access file to process
 -ansi,--spring.output.ansi.enabled <arg>
 -c,--clear                                 empty database before
                                            processing
 -duration,--duration <arg>                 Duration interval (hourly or
                                            daily)
 -h,--help                                  show help.
 -q,--query                                 no processing sink, just query
 -startDate,--startDate <arg>               Start Date in
 -threshold,--threshold <arg>               Threshold limit (integer >0)
 -url,--spring.datasource.url <arg>         database url
```

## Sql

Sqls are provided inside `/src/main/resources/application.properties` and `/src/main/resources/db/migration`

## Privileges

Should have root privileges for bulk optimizations

## Defaults

Sensible defaults are provide inside `/src/main/resources/application.properties` including database url and others