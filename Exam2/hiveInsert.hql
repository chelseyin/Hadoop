USE ${hivevar:databaseName};
LOAD DATA INPATH 'hdfs://hadoop-m1.us-central1-f.c.halogen-choir-235521.internal:8020/user/root/tmp/Exams/yinc/part-r-00000' OVERWRITE into table ${hivevar:tempTableName} Partition(username='yinc');
Set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${hivevar:tableName} Partition(username) SELECT name, cno, cname, grade, username from ${hivevar:tempTableName};