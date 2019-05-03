package org.somepackage
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import it.nerdammer.spark.hbase._
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.hive.HiveContext
 
object BookScanner {
   def read(jsc: JavaSparkContext, sqlContext: HiveContext): DataFrame = {
    val sc = JavaSparkContext.toSparkContext(jsc)
 
    val hBaseRDD = sc.hbaseTable[(String, String)]("books")
      .select("author")
      .inColumnFamily("info")
      .withStartRow("A")
      .withStopRow("H")
     
    return sqlContext.createDataFrame(hBaseRDD);
  }
}