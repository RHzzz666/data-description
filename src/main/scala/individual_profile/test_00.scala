//package individual_profile
//
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.{DataFrame, SparkSession,functions}
//
//object test_00 {
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder()
//      .appName("behavior record")
//      .master("local")
//      .getOrCreate()
//    import spark.implicits._
//    val get_id = functions.udf(string_last_3_char _)
//
//    //商品数据
//    def goods_catalog =
//      s"""{
//         |"table":{"namespace":"default", "name":"tbl_goods"},
//         |"rowkey":"id",
//         |"columns":{
//         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
//         |"c_orderSn":{"cf":"cf", "col":"cOrderSn", "type":"string"},
//         |"product_type":{"cf":"cf", "col":"productType", "type":"string"},
//         |"product_name":{"cf":"cf", "col":"productName", "type":"string"}
//         |}
//         |}""".stripMargin
//    val goods_df: DataFrame = spark.read
//      .option(HBaseTableCatalog.tableCatalog, goods_catalog)
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .load()
//
//    def orders_catalog =
//      s"""{
//         |"table":{"namespace":"default", "name":"tbl_orders"},
//         |"rowkey":"id",
//         |"columns":{
//         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
//         |"member_id":{"cf":"cf", "col":"memberId", "type":"string"},
//         |"payment_status":{"cf":"cf", "col":"paymentStatus", "type":"string"},
//         |"modified":{"cf":"cf", "col":"modified", "type":"string"},
//         |"payment_name":{"cf":"cf", "col":"paymentName", "type":"string"},
//         |"order_status":{"cf":"cf", "col":"orderStatus", "type":"string"},
//         |"orderSn":{"cf":"cf", "col":"orderSn", "type":"string"},
//         |"order_amount":{"cf":"cf", "col":"orderAmount", "type":"string"},
//         |"finish_time":{"cf":"cf", "col":"finishTime", "type":"string"}
//         |}
//         |}""".stripMargin
//    val orders_df:DataFrame =spark.read
//      .option(HBaseTableCatalog.tableCatalog, orders_catalog)
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .load()
//
//    //物品
//    val goods_info_df= goods_df.select('c_orderSn,'product_name)
//    val order_info_df = orders_df.select('orderSn,'member_id,'order_status)
//
//    val good_order_df:DataFrame =
//  }
//
//  def string_last_3_char(str:String):String = {
//    val len = str.length
//    if(len > 3) str.substring(len-3,len) else str
//  }
//}
