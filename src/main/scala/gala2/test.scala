package gala2

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import scala.util.parsing.json._
object test {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_orders"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |    "paymentCode":{"cf":"cf", "col":"paymentCode", "type":"string"}
         |  }
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    import spark.implicits._

    val source: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    //source.show(20, false)

    val result = source.groupBy('memberId, 'paymentCode)
      .agg(count('paymentCode) as "count")
      .withColumn("row_num", row_number() over Window.partitionBy('memberId).orderBy('count.desc))
      .where('row_num === 1)
      .withColumnRenamed("memberId", "id").drop("count", "row_num")

    //result.show( false)


    def goodsStatus =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_orders"},
         |  "rowkey":"id",
         |  "columns":{
         |  "id":{"cf":"rowkey","col":"id","type":"long"},
         |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |    "orderStatus":{"cf":"cf", "col":"orderStatus", "type":"string"}
         |  }
         |}""".stripMargin
    val status: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, goodsStatus)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    //atus.show(20)
    val result_bill =status.withColumn("Id",substring('memberId,-3,3))
      .groupBy('Id)
      .agg(count('Id) as "billCount",
        sum(when('orderStatus==="1",1).otherwise(0))as "Change",//1表示换货
        sum(when('orderStatus==="0",1).otherwise(0))as "Return")//0表示退货
    result_bill.drop('id).withColumn("SumOfChangeAndReturn",'Change + 'Return)
      .show(false)
    val shoppingRoute=
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_orders"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |    "payTime":{"cf":"cf", "col":"payTime", "type":"string"},
         |    "orderStatus":{"cf":"cf", "col":"orderStatus", "type":"string"}
         |  }
         |}""".stripMargin
    val route_status: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, shoppingRoute)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val result_route=route_status.withColumn("Id",'memberId)
      .where('orderStatus==="202")
      .groupBy('Id,'payTime,'orderStatus)
      .agg(count('Id)as 'shoppingTime)
      //.orderBy('shoppingTime.desc)
      .withColumn("PayTime",from_unixtime('payTime))
      .withColumn("payTimestamp",unix_timestamp('PayTime))
      .withColumn("payTimeInt",'payTimestamp.cast("long"))
      //.withColumn("LastShopping",max('payTimeInt))
      //.withColumn("FirstShopping",min('payTimeInt))
      //.withColumn("TimeRoute",(max('PayTime)-min('PayTime))/'shoppintTime)
      //.orderBy('PayTime)
      .show()
  }
}