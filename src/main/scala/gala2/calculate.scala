package gala2

import gala2.read.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.dsl.expressions.DslSymbol
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{col, count, monotonically_increasing_id, to_timestamp, when, year}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
object calculate {

  def main(args: Array[String]): Unit = {
  //  discount()
   // cast()


  }

  def cast():Unit=
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"cast":{"cf":"cf", "col":"消费能力", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val s4=df.groupBy("cast")
      .agg(count("cast")as "cast_count").sort(col("cast_count"))
      .withColumn("id",monotonically_increasing_id)
    def catalogwrite =
      s"""{
         |"table":{"namespace":"default","name":"cast"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"cast":{"cf":"cf", "col":"cast", "type":"string"},
         |"cast_count":{"cf":"cf", "col":"cast_count", "type":"long"}
         |}
         |}""".stripMargin
    s4.write
      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  def discount():Unit=
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"discount":{"cf":"cf", "col":"消费优惠券依赖度", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val s4=df.groupBy("discount")
      .agg(count("discount")as "discount_count").sort(col("discount_count"))
      .withColumn("id",monotonically_increasing_id)
    def catalogwrite =
      s"""{
         |"table":{"namespace":"default","name":"discount"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"discount":{"cf":"cf", "col":"discount", "type":"string"},
         |"discount_count":{"cf":"cf", "col":"discount_count", "type":"long"}
         |}
         |}""".stripMargin
    s4.write
      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
  def job_brand_preference(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"brand_preference":{"cf":"cf", "col":"brand_preference", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jobs=df.select(col("job").as("job"),col("brand_preference"))
      .where(col("brand_preference")=!="其他")
    val s4=jobs.groupBy("job","brand_preference")
      .agg(count("brand_preference")as "brand_preference_count").sort(col("brand_preference"),col("job"))
      .withColumn("id",monotonically_increasing_id)
    def catalogwrite =
      s"""{
         |"table":{"namespace":"default","name":"job_brand_preference"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"brand_preference":{"cf":"cf", "col":"brand_preference", "type":"string"},
         |"brand_preference_count":{"cf":"cf", "col":"brand_preference_count", "type":"long"}
         |}
         |}""".stripMargin
    s4.write
      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    s4.toJSON.collectAsList().toString
  }

  def job_payment_way(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"payment_way":{"cf":"cf", "col":"payment_way", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jobs=df.select(col("job").as("job"),col("payment_way"))
    val s4=jobs.groupBy("job","payment_way")
      .agg(count("payment_way")as "payment_way_count").sort(col("payment_way"),col("job"))
      .withColumn("id",monotonically_increasing_id)
    def catalogwrite =
      s"""{
         |"table":{"namespace":"default","name":"job_payment_way"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"payment_way":{"cf":"cf", "col":"payment_way", "type":"string"},
         |"payment_way_count":{"cf":"cf", "col":"payment_way_count", "type":"long"}
         |}
         |}""".stripMargin
    s4.write
      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    s4.toJSON.collectAsList().toString

  }

  def job_shopping_cycle(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"shopping_cycle":{"cf":"cf", "col":"shopping_cycle", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jobs=df.select(col("job").as("job"),col("shopping_cycle"))
    val s4=jobs.groupBy("job","shopping_cycle")
      .agg(count("shopping_cycle")as "shopping_cycle_count").sort(col("shopping_cycle"),col("job"))
      .withColumn("id",monotonically_increasing_id)
    def catalogwrite =
      s"""{
         |"table":{"namespace":"default","name":"job_shopping_cycle"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"shopping_cycle":{"cf":"cf", "col":"shopping_cycle", "type":"string"},
         |"shopping_cycle_count":{"cf":"cf", "col":"shopping_cycle_count", "type":"long"}
         |}
         |}""".stripMargin
    s4.write
      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    return s4.toJSON.collectAsList().toString
  }

  def job_ave_price_range(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"ave_price_range":{"cf":"cf", "col":"ave_price_range", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jobs=df.select(col("job").as("job"),col("ave_price_range"))
    val s4=jobs.groupBy("job","ave_price_range")
      .agg(count("ave_price_range")as "ave_price_range_count").sort(col("ave_price_range"),col("job"))
      .withColumn("id",monotonically_increasing_id)
    def catalogwrite =
      s"""{
         |"table":{"namespace":"default","name":"job_ave_price_range"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"ave_price_range":{"cf":"cf", "col":"ave_price_range", "type":"string"},
         |"ave_price_range_count":{"cf":"cf", "col":"ave_price_range_count", "type":"long"}
         |}
         |}""".stripMargin
    s4.write
      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    s4.toJSON.collectAsList().toString
  }

  def job_order_highest_range(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"order_highest_range":{"cf":"cf", "col":"order_highest_range", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jobs=df.select(col("job").as("job"),col("order_highest_range"))
    val s4=jobs.groupBy("job","order_highest_range")
      .agg(count("order_highest_range")as "order_highest_range_count").sort(col("order_highest_range"),col("job"))
      .withColumn("id",monotonically_increasing_id)
    def catalogwrite =
      s"""{
         |"table":{"namespace":"default","name":"job_order_highest_range"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"order_highest_range":{"cf":"cf", "col":"order_highest_range", "type":"string"},
         |"order_highest_range_count":{"cf":"cf", "col":"order_highest_range_count", "type":"long"}
         |}
         |}""".stripMargin
    s4.write
      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    s4.toJSON.collectAsList().toString
  }

  def job_frequency_range(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"frequency_range":{"cf":"cf", "col":"frequency_range(高,中,低)", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jobs=df.select(col("job").as("job"),col("frequency_range"))
    val s4=jobs.groupBy("job","frequency_range")
      .agg(count("frequency_range")as "frequency_range_count").sort(col("frequency_range"),col("job"))
      .withColumn("id",monotonically_increasing_id)
    def catalogwrite =
      s"""{
         |"table":{"namespace":"default","name":"job_frequency_range"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"frequency_range":{"cf":"cf", "col":"frequency_range", "type":"string"},
         |"frequency_range_count":{"cf":"cf", "col":"frequency_range_count", "type":"long"}
         |}
         |}""".stripMargin
    s4.write
      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    s4.toJSON.collectAsList().toString
  }
  def job_exchange_item_rate(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"exchange_item_rate":{"cf":"cf", "col":"exchange_item_rate(高,中,低)", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jobs=df.select(col("job").as("job"),col("exchange_item_rate"))
    val s4=jobs.groupBy("job","exchange_item_rate")
      .agg(count("exchange_item_rate")as "exchange_item_rate_count").sort(col("exchange_item_rate"),col("job"))
      .withColumn("id",monotonically_increasing_id)
    def catalogwrite =
      s"""{
         |"table":{"namespace":"default","name":"job_exchange_item_rate"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"exchange_item_rate":{"cf":"cf", "col":"exchange_item_rate", "type":"string"},
         |"exchange_item_rate_count":{"cf":"cf", "col":"exchange_item_rate_count", "type":"long"}
         |}
         |}""".stripMargin
    s4.write
      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    s4.toJSON.collectAsList().toString
  }

  def job_return_item_rate(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"return_item_rate":{"cf":"cf", "col":"return_item_rate(高,中,低)", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jobs=df.select(col("job").as("job"),col("return_item_rate"))
    val s4=jobs.groupBy("job","return_item_rate")
      .agg(count("return_item_rate")as "return_item_rate_count").sort(col("return_item_rate"),col("job"))
      .withColumn("id",monotonically_increasing_id)
    def catalogwrite =
      s"""{
         |"table":{"namespace":"default","name":"job_return_item_rate"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"return_item_rate":{"cf":"cf", "col":"return_item_rate", "type":"string"},
         |"return_item_rate_count":{"cf":"cf", "col":"return_item_rate_count", "type":"long"}
         |}
         |}""".stripMargin
    s4.write
      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    s4.toJSON.collectAsList().toString
  }
  def birth_brand_preference(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"age_class":{"cf":"cf","col":"age_class","type":"string"},
         |"brand_preference":{"cf":"cf", "col":"brand_preference", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val births=df.select(col("age_class").as("birth"),col("brand_preference"))
      .where(col("brand_preference")=!="其他")
    val s4=births.groupBy("birth","brand_preference")
      .agg(count("brand_preference")as "brand_preference_count").sort(col("brand_preference"),col("birth"))
      .withColumn("id",monotonically_increasing_id)
    def catalogwrite =
      s"""{
         |"table":{"namespace":"default","name":"birth_brand_preference"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"birth":{"cf":"cf","col":"birth","type":"string"},
         |"brand_preference":{"cf":"cf", "col":"brand_preference", "type":"string"},
         |"brand_preference_count":{"cf":"cf", "col":"brand_preference_count", "type":"long"}
         |}
         |}""".stripMargin
    s4.write
      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    s4.toJSON.collectAsList().toString
  }

  def birth_payment_way(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"age_class":{"cf":"cf","col":"age_class","type":"string"},
         |"payment_way":{"cf":"cf", "col":"payment_way", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val births=df.select(col("age_class").as("birth"),col("payment_way"))
    val s4=births.groupBy("birth","payment_way")
      .agg(count("payment_way")as "payment_way_count").sort(col("payment_way"),col("birth"))
      .withColumn("id",monotonically_increasing_id)
    def catalogwrite =
      s"""{
         |"table":{"namespace":"default","name":"birth_payment_way"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"birth":{"cf":"cf","col":"birth","type":"string"},
         |"payment_way":{"cf":"cf", "col":"payment_way", "type":"string"},
         |"payment_way_count":{"cf":"cf", "col":"payment_way_count", "type":"long"}
         |}
         |}""".stripMargin
    s4.write
      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    s4.toJSON.collectAsList.toString

  }

  def birth_shopping_cycle(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"age_class":{"cf":"cf","col":"age_class","type":"string"},
         |"shopping_cycle":{"cf":"cf", "col":"shopping_cycle", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val births=df.select(col("age_class").as("birth"),col("shopping_cycle"))
    val s4=births.groupBy("birth","shopping_cycle")
      .agg(count("shopping_cycle")as "shopping_cycle_count").sort(col("shopping_cycle"),col("birth"))
      .withColumn("id",monotonically_increasing_id)
    def catalogwrite =
      s"""{
         |"table":{"namespace":"default","name":"birth_shopping_cycle"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"birth":{"cf":"cf","col":"birth","type":"string"},
         |"shopping_cycle":{"cf":"cf", "col":"shopping_cycle", "type":"string"},
         |"shopping_cycle_count":{"cf":"cf", "col":"shopping_cycle_count", "type":"long"}
         |}
         |}""".stripMargin
    s4.write
      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    s4.toJSON.collectAsList.toString
  }

  def birth_ave_price_range(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"age_class":{"cf":"cf","col":"age_class","type":"string"},
         |"ave_price_range":{"cf":"cf", "col":"ave_price_range", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val births=df.select(col("age_class").as("birth"),col("ave_price_range"))
    val s4=births.groupBy("birth","ave_price_range")
      .agg(count("ave_price_range")as "ave_price_range_count").sort(col("ave_price_range"),col("birth"))
      .withColumn("id",monotonically_increasing_id)
    def catalogwrite =
      s"""{
         |"table":{"namespace":"default","name":"birth_ave_price_range"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"birth":{"cf":"cf","col":"birth","type":"string"},
         |"ave_price_range":{"cf":"cf", "col":"ave_price_range", "type":"string"},
         |"ave_price_range_count":{"cf":"cf", "col":"ave_price_range_count", "type":"long"}
         |}
         |}""".stripMargin
    s4.write
      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    s4.toJSON.collectAsList().toString
  }

  def birth_order_highest_range(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"age_class":{"cf":"cf","col":"age_class","type":"string"},
         |"order_highest_range":{"cf":"cf", "col":"order_highest_range", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val births=df.select(col("age_class").as("birth"),col("order_highest_range"))
    val s4=births.groupBy("birth","order_highest_range")
      .agg(count("order_highest_range")as "order_highest_range_count").sort(col("order_highest_range"),col("birth"))
      .withColumn("id",monotonically_increasing_id)
    def catalogwrite =
      s"""{
         |"table":{"namespace":"default","name":"birth_order_highest_range"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"birth":{"cf":"cf","col":"birth","type":"string"},
         |"order_highest_range":{"cf":"cf", "col":"order_highest_range", "type":"string"},
         |"order_highest_range_count":{"cf":"cf", "col":"order_highest_range_count", "type":"long"}
         |}
         |}""".stripMargin
    s4.write
      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    s4.toJSON.collectAsList().toString
  }

  def birth_frequency_range(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"age_class":{"cf":"cf","col":"age_class","type":"string"},
         |"frequency_range":{"cf":"cf", "col":"frequency_range(高,中,低)", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val births=df.select(col("age_class").as("birth"),col("frequency_range"))

    val s4=births.groupBy("birth","frequency_range")
      .agg(count("frequency_range")as "frequency_range_count").sort(col("frequency_range"),col("birth"))
      .withColumn("id",monotonically_increasing_id)

    def catalogwrite =
      s"""{
         |"table":{"namespace":"default","name":"birth_frequency_range"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"birth":{"cf":"cf","col":"birth","type":"string"},
         |"frequency_range":{"cf":"cf", "col":"frequency_range", "type":"string"},
         |"frequency_range_count":{"cf":"cf", "col":"frequency_range_count", "type":"long"}
         |}
         |}""".stripMargin
    s4.write
      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()


    s4.toJSON.collectAsList().toString
  }
  def birth_exchange_item_rate(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"age_class":{"cf":"cf","col":"age_class","type":"string"},
         |"exchange_item_rate":{"cf":"cf", "col":"exchange_item_rate(高,中,低)", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val births=df.select(col("age_class").as("birth"),col("exchange_item_rate"))

    val s4=births.groupBy("birth","exchange_item_rate")
      .agg(count("exchange_item_rate")as "exchange_item_rate_count").sort(col("exchange_item_rate"),col("birth"))
      .withColumn("id",monotonically_increasing_id)
    def catalogwrite =
      s"""{
         |"table":{"namespace":"default","name":"birth_exchange_item_rate"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"birth":{"cf":"cf","col":"birth","type":"string"},
         |"exchange_item_rate":{"cf":"cf", "col":"exchange_item_rate", "type":"string"},
         |"exchange_item_rate_count":{"cf":"cf", "col":"exchange_item_rate_count", "type":"long"}
         |}
         |}""".stripMargin
    s4.write
      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    s4.toJSON.collectAsList().toString
  }

  def birth_return_item_rate(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"age_class":{"cf":"cf","col":"age_class","type":"string"},
         |"return_item_rate":{"cf":"cf", "col":"return_item_rate(高,中,低)", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val births=df.select(col("age_class").as("birth"),col("return_item_rate"))
    val s4=births.groupBy("birth","return_item_rate")
      .agg(count("return_item_rate")as "return_item_rate_count").sort(col("return_item_rate"),col("birth"))

    def catalogwrite =
      s"""{
         |"table":{"namespace":"default","name":"birth_return_item_rate"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"birth":{"cf":"cf","col":"birth","type":"string"},
         |"return_item_rate":{"cf":"cf", "col":"return_item_rate", "type":"string"},
         |"return_item_rate_count":{"cf":"cf", "col":"return_item_rate_count", "type":"long"}
         |}
         |}""".stripMargin
    s4.write
      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    s4.toJSON.collectAsList().toString
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
