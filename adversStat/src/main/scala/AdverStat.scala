

import java.util.Date

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.DateUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object AdverStat {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("adver").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // val streamingContext = StreamingContext.getActiveOrCreate(checkpointDir, func)
    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))

    val kafka_brokers = ConfigurationManager.config.getString(Constants.KAFKA_BROKERS)
    val kafka_topics = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)

    val kafkaParam = Map(
      "bootstrap.servers" -> kafka_brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      // auto.offset.reset
      // latest: 先去Zookeeper获取offset，如果有，直接使用，如果没有，从最新的数据开始消费；
      // earlist: 先去Zookeeper获取offset，如果有，直接使用，如果没有，从最开始的数据开始消费
      // none: 先去Zookeeper获取offset，如果有，直接使用，如果没有，直接报错
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // adRealTimeDStream: DStream[RDD RDD RDD ...]  RDD[message]  message: key value
    val adRealTimeDStream = KafkaUtils.createDirectStream[String, String](streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam)
    )

    // 取出了DSream里面每一条数据的value值
    // adReadTimeValueDStream: Dstram[RDD  RDD  RDD ...]   RDD[String]
    // String:  timestamp province city userid adid
    val adReadTimeValueDStream: DStream[String] = adRealTimeDStream.map(item => item.value())

    val adRealTimeFilterDStream: DStream[String] = adReadTimeValueDStream.transform {
      logRDD =>

        // blackListArray: Array[AdBlacklist]     AdBlacklist: userId
        val blackListArray = AdBlacklistDAO.findAll()

        // userIdArray: Array[Long]  [userId1, userId2, ...]
        val userIdArray = blackListArray.map(item => item.userid)

        logRDD.filter {
          // log : timestamp province city userid adid
          case log =>
            val logSplit = log.split(" ")
            val userId = logSplit(3).toLong
            !userIdArray.contains(userId)
        }
    }
    //
    streamingContext.checkpoint("./spark-streaming")

    adRealTimeFilterDStream.checkpoint(Duration(10000))

    // 需求一：实时维护黑名单 一天内点击次数大于 100 次的为 黑名单用户；
    generateBlackList(adRealTimeFilterDStream)


    // 需求二：各省各城市一天中的广告点击量（累积统计）

    val key2ProvinceCityCountDStream = provinceCityClickStat(adRealTimeFilterDStream)

    // 需求三：统计各省Top3热门广告(累计统计--天)

    provinceTop3Adver(sparkSession, key2ProvinceCityCountDStream)


    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def provinceTop3Adver(sparkSession: SparkSession, key2ProvinceCityCountDStream: DStream[(String, Long)]) = {

    // key2ProvinceCityCountDStream: [RDD[(key, count)]]
    // key: date_province_city_adid
    // key2ProvinceCountDStream: [RDD[(newKey, count)]]
    // newKey: date_province_adid
    val key2ProvinceCountDStream = key2ProvinceCityCountDStream.map {
      case (key, count) =>
        val keySplit = key.split("_")
        val date = keySplit(0)
        val province = keySplit(1)
        val adid = keySplit(3)

        val newKey = date + "_" + province + "_" + adid
        (newKey, count)
    }
    val key2ProvinceAggrCountDStream: DStream[(String, Long)] = key2ProvinceCountDStream.reduceByKey(_ + _)
    val top3DStream: DStream[Row] = key2ProvinceAggrCountDStream.transform { //从一个rdd 返回另一个rdd
      rdd =>
        // rdd:RDD[(key, count)]
        // key: date_province_adid
        val basicDateRDD: RDD[(String, String, Long, Long)] = rdd.map {
          case (key, count) =>
            val keySplit = key.split("_")
            val date = keySplit(0)
            val province = keySplit(1)
            val adid = keySplit(2).toLong
            (date, province, adid, count)
        }
        // 利用sparkCore和SparkSQL 进行求 各省份Top3 的广告点击量
        import sparkSession.implicits._
        basicDateRDD.toDF("date", "province", "adid", "count").createOrReplaceTempView("tmp_basic_info")

        val sql = "select date,province,adid,count from (" +
          "select date,province,adid,count," +
          "row_number() over (partition by province order by count desc) rank" +
          " from tmp_basic_info )  t where rank <=3"
        sparkSession.sql(sql).rdd
    }
    //DStream 写到数据库，需要经过下面这个步骤， froeachRDD 和 foreachPartition 需要用到
    top3DStream.foreachRDD {
      // rdd : RDD[row]
      rdd =>
        rdd.foreachPartition {
          items =>
            val top3Array = new ArrayBuffer[AdProvinceTop3]()
            for (item <- items) {
              val date = item.getAs[String]("date")
              val province = item.getAs[String]("province")
              val adid = item.getAs[Long]("adid")
              val count = item.getAs[Long]("count")
              top3Array += AdProvinceTop3(date, province, adid, count)
            }
            AdProvinceTop3DAO.updateBatch(top3Array.toArray)

        }
    }

  }


  def provinceCityClickStat(adRealTimeFilterDStream: DStream[String]) = {

    // adRealTimeFilterDStream: DStream[RDD[String]]    String -> log : timestamp province city userid adid
    // key2ProvinceCityDStream: DStream[RDD[key, 1L]]
    val key2ProvinceCityDStream = adRealTimeFilterDStream.map {
      case log =>
        val logSplit: Array[String] = log.split(" ")
        val timeStamp: Long = logSplit(0).toLong
        // dateKey : yyyyMMdd
        val dateKey = DateUtils.formatDateKey(new Date(timeStamp))
        val province = logSplit(1)
        val city = logSplit(2)
        val userId = logSplit(3)
        val adid = logSplit(4)
        val key = dateKey + "_" + province + "_" + city + "_" + adid
        (key, 1L)
    }
    val key2StateDStream: DStream[(String, Long)] = key2ProvinceCityDStream.updateStateByKey[Long] {
      (values: Seq[Long], state: Option[Long]) =>
        var newValue = 0L
        if (state.isDefined)
          newValue += state.get
        for (value <- values) {
          newValue += value
        }
        Some(newValue)
    }

    key2StateDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items =>
            val adStateArray = new ArrayBuffer[AdStat]()
            for ((key, count) <- items) {
              val keySplit = key.split("_")
              val date = keySplit(0)
              val province = keySplit(1)
              val city = keySplit(2)
              val adid = keySplit(3).toLong

              adStateArray += AdStat(date, province, city, adid, count)
            }
            AdStatDAO.updateBatch(adStateArray.toArray)
        }
    }
    //需求三用到需求二的结果；
    key2StateDStream
  }


  def generateBlackList(adRealTimeFilterDStream: DStream[String]) = {
    // adRealTimeFilterDStream: DStream[RDD[String]]    String -> log : timestamp province city userid adid
    // key2NumDStream: [RDD[(key, 1L)]]
    val key2NumDStream: DStream[(String, Long)] = adRealTimeFilterDStream.map {
      case log =>
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        // 格式 yyyymmdd
        val dateKey = DateUtils.formatDateKey(new Date(timeStamp))
        val userId = logSplit(3).toLong
        val adId = logSplit(4).toLong
        val key = dateKey + "_" + userId + "_" + adId
        (key, 1L)
    }
    val key2CountDStream: DStream[(String, Long)] = key2NumDStream.reduceByKey(_ + _)
    // 根据每一个RDD里面的数据，更新用户点击次数表
    key2CountDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items =>
            val clickCountArray = new ArrayBuffer[AdUserClickCount]()

            for ((key, count) <- items) {
              val keySplit = key.split("_")
              val date = keySplit(0)
              val userId = keySplit(1).toLong
              val adid = keySplit(2).toLong

              clickCountArray += AdUserClickCount(date, userId, adid, count)
            }

            AdUserClickCountDAO.updateBatch(clickCountArray.toArray)
        }
    }
    // key2BlackListDStream: DStream[RDD[(key, count)]]
    val key2BlackListDStream = key2CountDStream.filter {
      case (key, count) =>
        // (20200207_15_12,1)
        val keySplit = key.split("_")
        val date = keySplit(0)
        val userId = keySplit(1).toLong
        val adId = keySplit(2).toLong
        //ad_user_click_count
        val clickCount = AdUserClickCountDAO.findClickCountByMultiKey(date, userId, adId)
        if (clickCount > 100) {
          true
        } else {
          false
        }
    }

    val userIdDStream: DStream[Long] = key2BlackListDStream.map {
      case (key, count) =>
        key.split("_")(1).toLong
    }.transform(rdd => rdd.distinct)
    userIdDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items =>
            val userIdArray = new ArrayBuffer[AdBlacklist]()
            for (userId <- items) {
              userIdArray += AdBlacklist(userId)
            }
            AdBlacklistDAO.insertBatch(userIdArray.toArray)
        }

    }

  }


}
