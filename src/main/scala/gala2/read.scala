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
    def catalog: String = {
      s"""{
         |"table":{"namespace":"default","name":"tbl_users"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"long"},
         |"e_mail":{"cf":"cf","col":"email","type":"string"},
         |"user_name":{"cf":"cf","col":"username","type":"string"},
         |"register_time":{"cf":"cf","col":"registerTime","type":"string"},
         |"password":{"cf":"cf","col":"password","type":"string"},
         |"last_login_time":{"cf":"cf","col":"lastLoginTime","type":"string"},
         |"qq":{"cf":"cf","col":"qq","type":"string"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"mobile":{"cf":"cf","col":"mobile","type":"string"},
         |"political_face":{"cf":"cf","col":"politicalFace","type":"string"},
         |"birthday":{"cf":"cf","col":"birthday","type":"string"},
         |"nationality":{"cf":"cf","col":"nationality","type":"string"},
         |"marriage":{"cf":"cf","col":"marriage","type":"string"},
         |"money":{"cf":"cf","col":"money","type":"string"},
         |"money_pwd":{"cf":"cf","col":"moneyPwd","type":"string"},
         |"nick_name":{"cf":"cf","col":"nick_name","type":"string"},
         |"is_blackList":{"cf":"cf","col":"is_blackList","type":"string"}
         |}
         |}
       """.stripMargin
    }
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val df2=df.where(col("user_name") === a and (col("password") === b))
//    df2.show()
    if(df2.isEmpty) {
      return "error"
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