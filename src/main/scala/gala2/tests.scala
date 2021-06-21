package gala2

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

object tests {
  def newTask_do2() :String = {
    val spark = SparkSession.builder()
      .appName("shctest")
      .master("local")
      .getOrCreate()

        def catalog =
          s"""{
             |"table":{"namespace":"default", "name":"tbl_logs"},
             |"rowkey":"id",
             |"columns":{
             |"id":{"cf":"rowkey", "col":"id", "type":"string"},
             |"user_agent":{"cf":"cf", "col":"user_agent", "type":"string"}
             |}
             |}""".stripMargin

        val data: DataFrame = spark.read
          .option(HBaseTableCatalog.tableCatalog, catalog)
          .format("org.apache.spark.sql.execution.datasources.hbase")
          .load()

        val jsonStr = data.toJSON.collect()
    spark.stop()
    return catalog
  }
}