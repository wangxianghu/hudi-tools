package scala.com.xianghu.hudi

import org.apache.spark.sql.SparkSession
import org.junit.{After, Assert, Before, Test}
import org.apache.hudi.QuickstartUtils._

import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.config.HoodieWriteConfig._

class HoodieDemos extends {

  var spark: SparkSession = _
  val basePath = "hdfs://localhost:8020/hudi/data/test_01";
  val tableName = "nari"
  val dataGen = new DataGenerator

  @Before
  def init(): Unit = {
    spark = SparkSession
      .builder
      .master("local[4]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("HoodieUtil")
      .getOrCreate
  }

  @Test
  def insert(): Unit = {
    val inserts = convertToStringList(dataGen.generateInserts(10))
    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
    df.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY.key(), "ts").
      option(RECORDKEY_FIELD_OPT_KEY.key(), "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY.key(), "partitionpath").
      option(TABLE_NAME.key(), tableName).
      mode(Overwrite).
      save(basePath)
  }

  @Test
  def query(): Unit = {
    val tripsSnapshotDF = spark.
      read.
      format("hudi").
      load(basePath + "/*/*/*/*")
//      load(basePath + "/*")
    tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

    spark.sql("select * from  hudi_trips_snapshot")
      .show()
  }

  @Test
  def queryDfs(): Unit = {
    spark
      .read
      .parquet(basePath + "/americas/united_states/san_francisco/66725251-c0b1-41d3-b694-5f1e880f5aca-0_1-28-29_20210705153011.parquet")
      .show();
  }

  @After
  def tearDown(): Unit = {
    spark.stop()
  }
}
