USE ${hivevar:databaseName};
Create TABLE IF NOT EXISTS ${hivevar:tableName}(line string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
LOAD DATA INPATH '${hivevar:inputLocation}' OVERWRITE into table ${hivevar:tableName};
DROP FUNCTION IF EXISTS udfUpper;
DROP FUNCTION IF EXISTS udfStrip;
CREATE FUNCTION udfUpper AS 'edu.rosehulman.yinc.Upper' USING JAR 'hdfs://hadoop-m1.us-central1-f.c.halogen-choir-235521.internal:8020/user/root/yinc/Lab6/Task2/Lab6Task2-1.0-SNAPSHOT.jar';
CREATE FUNCTION udfSTRIP AS 'edu.rosehulman.yinc.Strip' USING JAR 'hdfs://hadoop-m1.us-central1-f.c.halogen-choir-235521.internal:8020/user/root/yinc/Lab6/Task2/Lab6Task2-1.0-SNAPSHOT.jar';
SELECT word, count(*) FROM ${hivevar:tableName} lateral view explode(split(udfUpper(udfStrip(line)),' ')) ltable AS word GROUP BY word;
