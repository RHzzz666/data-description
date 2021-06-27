package wcl_user

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object user_order_money {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Orders_info")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    def user_orders_info =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_orders"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"member_id":{"cf":"cf", "col":"memberId", "type":"string"},
         |"order_status":{"cf":"cf", "col":"orderStatus", "type":"string"},
         |"order_amount":{"cf":"cf", "col":"orderAmount", "type":"string"},
         |"finish_time":{"cf":"cf", "col":"finishTime", "type":"string"}
         |}
         |}""".stripMargin

    //用户，时间，金额
    val get_id = functions.udf(string_last_3_char _)
    val df: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, user_orders_info)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val basic_df = df.withColumn("member_id", get_id('member_id))
    val res_df=basic_df.where('order_status === "202")
      .drop(df.col("order_status"))
      .withColumn("shopping_time", from_unixtime('finish_time))
      .na.drop("all")
      .drop(df.col("finish_time"))


    def user_orders_info_simple_write =
      s"""{
         |"table":{"namespace":"default", "name":"user_orders_info_simple"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"member_id":{"cf":"cf", "col":"member_id", "type":"string"},
         |"shopping_time":{"cf":"cf", "col":"shopping_time", "type":"string"},
         |"order_amount":{"cf":"cf", "col":"order_amount", "type":"string"}
         |}
         |}""".stripMargin


    res_df.write
      .option(HBaseTableCatalog.tableCatalog, user_orders_info_simple_write)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  def string_last_3_char(str: String): String = {
    val len = str.length
    val sub_str = if (len > 3) str.substring(len - 3, len) else str
    if (sub_str(0) == '0' && sub_str(1) == '0') sub_str.substring(2)
    else if (sub_str(0) == '0') sub_str.substring(1, 3)
    else sub_str
  }
}
