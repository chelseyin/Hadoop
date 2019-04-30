USE ${hivevar:databaseName};
Create TABLE IF NOT EXISTS ${hivevar:tableName}(line string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
LOAD DATA INPATH '${hivevar:inputLocation}' OVERWRITE into table ${hivevar:tableName};
DROP FUNCTION IF EXISTS udfUpper;
DROP FUNCTION IF EXISTS udfStrip;
CREATE FUNCTION udfUpper AS 'edu.rosehulman.yinc.Upper' USING JAR '/user/root/yinc/Lab6/Task2/Lab6Task2-1.0-SNAPSHOT.jar';
CREATE FUNCTION STRIP AS 'edu.rosehulman.yinc.Strip' USING JAR '/user/root/yinc/Lab6/Task2/Lab6Task2-1.0-SNAPSHOT.jar' 
SELECT word, count(*) FROM ${hivevar:tableName} lateral view explode(split(udfUpper(udfStrip(line)),' ')) ltable AS word GROUP BY word;
