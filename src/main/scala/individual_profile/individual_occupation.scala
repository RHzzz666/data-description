package individual_profile

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.{DataFrame, SparkSession}

object individual_occupation {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_users"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"job":{"cf":"cf", "col":"job", "type":"string"}
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

    //就业状况：1事业单位、2在职、3待业、4自主创业
    val result = readDF.select('id,
      when('job === "1", "学生")
        .when('job === "2", "公务员")
        .when('job === "3", "军人")
        .when('job === "4", "警察")
        .when('job==="5","教师")
        .when('job==="6","白领")
        .otherwise("其他职业")
        .as("occupation")
    )
    result.printSchema()
    result.show(false)

//    println("开始写入职业状态")
//    def catalogWrite =
//      s"""{
//         |"table":{"namespace":"default", "name":"user_profile"},
//         |"rowkey":"id",
//         |"columns":{
//         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
//         |"occupation":{"cf":"cf", "col":"occupation", "type":"string"}
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
