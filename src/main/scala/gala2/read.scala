package gala2

import gala2.read.spark
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.col
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.util.parsing.json.{JSON, JSONArray, JSONObject}

object read {
  val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
  import spark.implicits._
  def Init():Unit={
    var ata=getgender()
  }

  def main(args: Array[String]): Unit = {
    val a=getperson("督咏","7f930f90eb6604e837db06908cc95149")
    println("result:")
    println(a)
  }

  def getperson(a:String,b:String):String={
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_users"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"avatarImageFileId":{"cf":"cf", "col":"avatarImageFileId", "type":"string"},
         |"email":{"cf":"cf", "col":"email", "type":"string"},
         |"username":{"cf":"cf", "col":"username", "type":"string"},
         |"password":{"cf":"cf", "col":"password", "type":"string"},
         |"salt":{"cf":"cf", "col":"salt", "type":"string"},
         |"registerTime":{"cf":"cf", "col":"registerTime", "type":"long"},
         |"lastLoginTime":{"cf":"cf", "col":"lastLoginTime", "type":"long"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()

    val df2=df.where(col("username") === a and (col("password") === b))
//    df2.show()
    if(df2.isEmpty) {
      return "err"
    }else{
      df2.toJSON.collectAsList().toString
    }
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
    val jsonData: Array[JSONObject] = df2Array.map { i =>
      new JSONObject(Map(i._1 -> i._2))
    }
    val jsTest = jsonData.mkString(",").replace("},{",",")
    // df.toJSON.collectAsList().toString
    jsTest
  }

}