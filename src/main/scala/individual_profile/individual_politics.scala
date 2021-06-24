package individual_profile

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object individual_politics {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_users"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"politicalFace":{"cf":"cf", "col":"politicalFace", "type":"string"}
         |}
         |}""".stripMargin

    import spark.implicits._

    val readDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val result = readDF.select('id,
      when('politicalFace === "1", "群众")
        .when('politicalFace === "2", "党员")
        .when('politicalFace === "3", "无党派人士")
        .otherwise("其他")
        .as("politics")
    )

    result.printSchema()
    result.show(false)
//    def catalogWrite =
//      s"""{
//         |"table":{"namespace":"default", "name":"user_profile"},
//         |"rowkey":"id",
//         |"columns":{
//         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
//         |"politics":{"cf":"cf", "col":"politics", "type":"string"}
//         |}
//         |}""".stripMargin
//    println("开始写入政治面貌信息")
//    result.write
//      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
//      .option(HBaseTableCatalog.newTable, "5")
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .save()
//    println("写入结束")
  }
}
