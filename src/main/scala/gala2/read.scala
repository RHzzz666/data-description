package gala2

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.dsl.expressions.DslSymbol
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.when
import org.json.{JSONArray, JSONObject}

object read {
  def getgender():String={
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"gender_sum"},
         |"rowkey":"gender",
         |"columns":{
         |"gender":{"cf":"rowkey", "col":"gender", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()
    import spark.implicits._
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val jsonStr:String = df.toJSON.collectAsList.toString
    return jsonStr
  }

  def getjob():String={
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"cal_job"},
         |"rowkey":"job",
         |"columns":{
         |"job":{"cf":"rowkey", "col":"job", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()
    import spark.implicits._
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val jsonStr:String = df.toJSON.collectAsList.toString
    return jsonStr
  }

  def getbirth():String={
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"cal_birth"},
         |"rowkey":"birth",
         |"columns":{
         |"birth":{"cf":"rowkey", "col":"birth", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()
    import spark.implicits._
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val jsonStr:String = df.toJSON.collectAsList.toString
    return jsonStr
  }

  /*
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"gender_sum"},
         |"rowkey":"gender",
         |"columns":{
         |"gender":{"cf":"rowkey", "col":"gender", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
      .show(false)


  }*/

}