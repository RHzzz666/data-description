package gala2

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.dsl.expressions.DslSymbol
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{col, count, to_timestamp, when, year}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
object calculate {

  def main(args: Array[String]): Unit = {
   // region_cal()
  //  marriage_cal()
   // nationality_cal()
   // political_face_cal()
  //  brand_preference_cal()
  //  consumption_ablity_cal()
 //   shopping_cycle_cal()
  //  ave_price_range_cal()
  //  order_highest_range_cal()
 //   log_frequency_cal()
  }


  def brand_preference_cal(): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"brand_preference":{"cf":"cf","col":"brand_preference","type":"string"}
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
    val s2=rdf.where(col("brand_preference")=!="其他").groupBy("brand_preference").count()
    val s3=s2.select("brand_preference","count")

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default","name":"cal_brand_preference"},
         |"rowkey":"brand_preference",
         |"columns":{
         |"brand_preference":{"cf":"rowkey", "col":"brand_preference", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    s3.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

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

  def region_cal(): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"region":{"cf":"cf","col":"region","type":"string"}
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
    val s2=rdf.groupBy("region").count()
    val s3=s2.select("region","count")

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default","name":"cal_region"},
         |"rowkey":"region",
         |"columns":{
         |"region":{"cf":"rowkey", "col":"region", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    s3.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  def marriage_cal(): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"marriage":{"cf":"cf","col":"marriage","type":"string"}
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
    val s2=rdf.groupBy("marriage").count()
    val s3=s2.select("marriage","count")

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default","name":"cal_marriage"},
         |"rowkey":"marriage",
         |"columns":{
         |"marriage":{"cf":"rowkey", "col":"marriage", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    s3.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  def nationality_cal(): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"nationality":{"cf":"cf","col":"nationality","type":"string"}
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
    val s2=rdf.groupBy("nationality").count()
    val s3=s2.select("nationality","count")

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default","name":"cal_nationality"},
         |"rowkey":"nationality",
         |"columns":{
         |"nationality":{"cf":"rowkey", "col":"nationality", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    s3.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  def political_face_cal(): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"political_face":{"cf":"cf","col":"political_face","type":"string"}
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
    val s2=rdf.groupBy("political_face").count()
    val s3=s2.select("political_face","count")

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default","name":"cal_political_face"},
         |"rowkey":"political_face",
         |"columns":{
         |"political_face":{"cf":"rowkey", "col":"political_face", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    s3.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  def consumption_ablity_cal(): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"consumption_ablity":{"cf":"cf","col":"consumption_ablity","type":"string"}
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
    val s2=rdf.groupBy("consumption_ablity").count()
    val s3=s2.select("consumption_ablity","count")

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default","name":"cal_consumption_ablity"},
         |"rowkey":"consumption_ablity",
         |"columns":{
         |"consumption_ablity":{"cf":"rowkey", "col":"consumption_ablity", "type":"string"},
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
      when('year >= 1950 && 'year < 1960, "五零后")
        .when('year >= 1960 && 'year < 1970, "六零后")
        .when('year >= 1970 && 'year < 1980, "七零后")
        .when('year >= 1980 && 'year < 1990, "八零后")
        .when('year >= 1990 && 'year < 2000, "九零后")
        .when('year >= 2000 && 'year < 2010, "零零后")
        .when('year >= 2010 && 'year < 2020, "一零后")
        .when('year >= 2020 && 'year < 2030, "二零后")
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
         |"birth":{"cf":"cf", "col":"birth", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    s5.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  def shopping_cycle_cal(): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"shopping_cycle":{"cf":"cf","col":"shopping_cycle","type":"string"}
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
    val s2=rdf.where(col("shopping_cycle")=!="其他").groupBy("shopping_cycle").count()
    val s3=s2.select("shopping_cycle","count")

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default","name":"cal_shopping_cycle"},
         |"rowkey":"shopping_cycle",
         |"columns":{
         |"shopping_cycle":{"cf":"rowkey", "col":"shopping_cycle", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    s3.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  def ave_price_range_cal(): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"ave_price_range":{"cf":"cf","col":"ave_price_range","type":"string"}
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
    val s2=rdf.where(col("ave_price_range")=!="其他").groupBy("ave_price_range").count()
    val s3=s2.select("ave_price_range","count")

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default","name":"cal_ave_price_range"},
         |"rowkey":"ave_price_range",
         |"columns":{
         |"ave_price_range":{"cf":"rowkey", "col":"ave_price_range", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    s3.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  def order_highest_range_cal(): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"order_highest_range":{"cf":"cf","col":"order_highest_range","type":"string"}
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
    val s2=rdf.where(col("order_highest_range")=!="其他").groupBy("order_highest_range").count()
    val s3=s2.select("order_highest_range","count")

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default","name":"cal_order_highest_range"},
         |"rowkey":"order_highest_range",
         |"columns":{
         |"order_highest_range":{"cf":"rowkey", "col":"order_highest_range", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    s3.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  def log_frequency_cal(): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"log_frequency":{"cf":"cf","col":"log_frequency","type":"string"}
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
    val s2=rdf.where(col("log_frequency")=!="其他").groupBy("log_frequency").count()
    val s3=s2.select("log_frequency","count")

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default","name":"cal_log_frequency"},
         |"rowkey":"log_frequency",
         |"columns":{
         |"log_frequency":{"cf":"rowkey", "col":"log_frequency", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    s3.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }


}
