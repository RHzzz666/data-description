package individual_profile

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.{DataFrame, SparkSession}

object individual_nationality {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_users"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"nationality":{"cf":"cf", "col":"nationality", "type":"string"}
         |}
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    import spark.implicits._

    val readDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    //1中国大陆、2中国香港、3中国澳门、4中国台湾、5其他
    val result = readDF.select('id,
      when('nationality === "1", "中国大陆")
        .when('nationality === "2", "中国香港")
        .when('nationality === "3", "中国澳门")
        .when('nationality === "4", "中国台湾")
        .when('nationality === "5", "其他")
        .otherwise("其他")
        .as("nationality")
    )
    result.printSchema()
    result.show(false)
//
//    println("开始写入国籍")
//    def catalogWrite =
//      s"""{
//         |"table":{"namespace":"default", "name":"user_profile"},
//         |"rowkey":"id",
//         |"columns":{
//         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
//         |"nationality":{"cf":"cf", "col":"nationality", "type":"string"}
//         |}
//         |}""".stripMargin
//
//    result.write
//      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
//      .option(HBaseTableCatalog.newTable, "5")
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .save()
  }
}
