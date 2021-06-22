package gala2

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.dsl.expressions.DslSymbol
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{col, to_timestamp, when, year}
object calculate {
  def gender_cal(): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_users"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"gender":{"cf":"cf", "col":"gender", "type":"string"}
         |}
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()
    import spark.implicits._
    val rdf:DataFrame=spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val result1s:DataFrame = rdf.select('id,
      when('gender === "1", "男")
        .when('gender === "2", "女")
        .otherwise("未知")
        .as("gender")
    )
    result1s.show(false)
    val s2=result1s.groupBy("gender").count()
    val s3=s2.select("gender","count")
    s3.show(false)

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"gender_sum"},
         |"rowkey":"gender",
         |"columns":{
         |"gender":{"cf":"rowkey", "col":"gender", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    s3.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  def job_cal():Unit={
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_users"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"job":{"cf":"cf", "col":"job", "type":"string"}
         |}
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()
    import spark.implicits._
    val rdf:DataFrame=spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val result1s:DataFrame = rdf.select('id,
      when('job === "1", "学生")
        .when('job === "2", "公务员")
        .when('job === "3", "军人")
        .when('job === "4", "警察")
        .when('job === "5", "教师")
        .when('job === "6", "白领")
        .otherwise("未知")
        .as("job")
    )
    result1s.show(false)
    val s2=result1s.groupBy("job").count()
    val s3=s2.select("job","count")
    s3.show(false)

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"cal_job"},
         |"rowkey":"job",
         |"columns":{
         |"job":{"cf":"rowkey", "col":"job", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    s3.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }


  def birth_cal(): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_users"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"birthday":{"cf":"cf", "col":"birthday", "type":"string"}
         |}
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()
    import spark.implicits._
    val rdf:DataFrame=spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val s3=rdf.select(year($"birthday").as("year"))
    val births=s3.select('year,
      when('year >= 1950 && 'year < 1960, "50后")
        .when('year >= 1960 && 'year < 1970, "60后")
        .when('year >= 1970 && 'year < 1980, "70后")
        .when('year >= 1980 && 'year < 1990, "80后")
        .when('year >= 1990 && 'year < 2000, "90后")
        .when('year >= 2000 && 'year < 2010, "00后")
        .when('year >= 2010 && 'year < 2020, "10后")
        .when('year >= 2020 && 'year < 2030, "20后")
        .otherwise("未知")
        .as("birth")
    )
    val s4=births.groupBy("birth").count()
    val s5=s4.select("birth","count")
    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"cal_birth"},
         |"rowkey":"birth",
         |"columns":{
         |"birth":{"cf":"rowkey", "col":"birth", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    s5.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()


  }

}
