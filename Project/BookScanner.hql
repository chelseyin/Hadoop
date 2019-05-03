CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`books_ext` (
    `title` string,
    `author` string,
    `year` int,
    `views` double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    'hbase.columns.mapping'=':key,info:author,info:year,analytics:views'
)
TBLPROPERTIES (
    'hbase.mapred.output.outputtable'='books',
    'hbase.table.name'='books'
);