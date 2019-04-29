CREATE DATABASE IF NOT EXISTS ${hiveconf:databaseName};
USE ${hiveconf:databaseName};
Create TABLE IF NOT EXISTS ${hiveconf:tableName}(year int,temp float,value int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' STORED AS TEXTFILE;
LOAD DATA INPATH ${hiveconf:inputLocation} OVERWRITE into table ${hiveconf:tableName};
SELECT year, min(temp) as mintemp, max(temp) as maxtemp,avg(temp) as avgtemp FROM ${hiveconf:tableName} WHERE value=1 OR value=0 GROUP BY year;
