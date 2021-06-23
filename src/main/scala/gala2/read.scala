package gala2

import gala2.read.spark
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.util.parsing.json.{JSON, JSONArray, JSONObject}

object read {
  val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
  import spark.implicits._
  def Init():Unit={
    var ata=getgender()
  }

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

    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    //println(df.toJSON.collectAsList().toString)
    val df2Array: Array[(String, Any)] = df.collect().map { row => (row(0).toString, row(1)) }
    println(df.collect())
    println(df2Array)
    val jsonData: Array[JSONObject] = df2Array.map { i =>
      new JSONObject(Map(i._1 -> i._2))
    }
    val jsTest = jsonData.mkString(",").replace("},{",",")
   // df.toJSON.collectAsList().toString
    jsTest
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
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    //val jsonStr:String = df.toJSON.collectAsList.toString
    //return jsonStr
    val df2Array: Array[(String, Any)] = df.collect().map { row => (row(0).toString, row(1)) }
    println(df.collect())
    println(df2Array)
    val jsonData: Array[JSONObject] = df2Array.map { i =>
      new JSONObject(Map(i._1 -> i._2))
    }
    val jsTest = jsonData.mkString(",").replace("},{",",")
    // df.toJSON.collectAsList().toString
    jsTest
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
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jsonStr:String = df.toJSON.collectAsList.toString
    val df2Array: Array[(String, Any)] = df.collect().map { row => (row(0).toString, row(1)) }
    println(df.collect())
    println(df2Array)
    val jsonData: Array[JSONObject] = df2Array.map { i =>
      new JSONObject(Map(i._1 -> i._2))
    }
    val jsTest = jsonData.mkString(",").replace("},{",",")
    // df.toJSON.collectAsList().toString
    jsTest
  }

}