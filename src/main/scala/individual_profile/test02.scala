package individual_profile

//基于tbl_orders
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession}

object test02 {
  //取后三个字符


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName("Order_info")
      .master("local")
      .getOrCreate()

    def order_info_catalog =
      """{
        |"table":{"namespace":"default","name":"tbl_orders"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"member_id:{"cf":"cf","col":"id","type":"string"}
        |}
        |}
      """.stripMargin

    val df: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, order_info_catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val string_last_3_char_fun = (str:String) => {
      val len = str.length
      if(len > 3) str.substring(len-3,len) else str
    }

//    val string_last_3_char = functions.udf(string_last_3_char_fun _)


  }




}
