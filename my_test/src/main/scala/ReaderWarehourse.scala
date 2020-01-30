import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext, sql}

object ReaderWarehourse {
  def main(args: Array[String]): Unit = {
    /*
    读取数仓中的数据，看看数据结构
     */
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ReaderWarehourse")
    //    val sc: SparkContext = new SparkContext(sparkConf)
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext
    val dataRDD: RDD[String] = sc.textFile("D:\\develop\\ideaWorkspace\\commerce_basic\\spark-warehouse\\user_info")
    dataRDD.collect().foreach(println)
    sc.stop()
  }

}
