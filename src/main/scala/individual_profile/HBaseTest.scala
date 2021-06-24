package individual_profile

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

object HBaseTest {
  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder()
      .master("local")
      .appName("HBaseTest")
      .getOrCreate()
    def catalog =
      s"""{
                     |"table":{"namespace":"default", "name":"tbl_users"},
                     |"rowkey":"id",
                     |"columns":{
                     |"id":{"cf":"rowkey", "col":"id", "type":"string"},
                     |"username":{"cf":"cf", "col":"username", "type":"string"}
                     |}
                     |}""".stripMargin
    //读表
    //默认空间下的name：tbl_users
    //主键：id
    //列：读取哪些列

    var df = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    //读取database 的 用户信息并转换成sparkSql的dataframe

    df.printSchema()
    df.show(20,false)
    spark.stop()
  }
}
