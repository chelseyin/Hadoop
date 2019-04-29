CREATE DATABASE IF NOT EXISTS ${hivevar:databaseName};
USE ${hivevar:databaseName};
Create TABLE IF NOT EXISTS ${hivevar:tableName}(year int,temp float,value int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
LOAD DATA INPATH '${hivevar:inputLocation}' OVERWRITE into table ${hivevar:tableName};
SELECT year, min(temp) as mintemp, max(temp) as maxtemp,avg(temp) as avgtemp FROM ${hivevar:tableName} WHERE value=1 OR value=0 GROUP BY year;
