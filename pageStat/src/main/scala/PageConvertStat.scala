import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.collection.mutable

object PageConvertStat {


  def main(args: Array[String]): Unit = {
    // 获取任务限制条件
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    // 获取唯一主键
    val taskUUID = UUID.randomUUID().toString
    // 创建sparkConf
    // 创建sparkSession
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("PageConvertStat")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    // 获取用户行为数据
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = getUserVisitAction(sparkSession, taskParam)

    //需要求的页面转化率
    val pageFlowStr = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    val pageFlowArray: Array[String] = pageFlowStr.split(",")
    //    Array[(String, String)] =
    val targetPageSplit = pageFlowArray.slice(0, pageFlowArray.length - 1).zip(pageFlowArray.tail).map {
      case (page1, page2) => page1 + "_" + page2
    }

    val sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey()
    //    sessionId2GroupRDD.foreach(println(_))
    val pageSplitNumRDD = sessionId2GroupRDD.flatMap {
      case (sessionId, iterableAction) =>
        val sortList: List[UserVisitAction] = iterableAction.toList.sortWith((uv1, uv2) => {
          DateUtils.parseTime(uv1.action_time).getTime < DateUtils.parseTime(uv2.action_time).getTime
        })
        val pageList: List[Long] = sortList.map {
          case action => action.page_id
        }
        val pageSplit: List[String] = pageList.slice(0, pageList.length - 1).zip(pageList.tail).map {
          case (page1, page2) => page1 + "_" + page2
        }
        val pageSplitFilter = pageSplit.filter {
          case pageSplit1 => targetPageSplit.contains(pageSplit1)
        }
        pageSplitFilter.map {
          case pageSplit => (pageSplit, 1L)
        }
    }
    //类似于reduceBykey, 结构不同
    val pageSplitCountMap: collection.Map[String, Long] = pageSplitNumRDD.countByKey()

    pageSplitCountMap.foreach(println(_))

    val startPage = pageFlowArray(0).toLong
    val startPageCount = sessionId2ActionRDD.filter {
      case (sessionId, action) => action.page_id == startPage
    }.count
    getPageConvert(sparkSession, taskUUID, targetPageSplit, startPageCount, pageSplitCountMap)
  }

  def getPageConvert(sparkSession: SparkSession,
                     taskUUID: String,
                     targetPageSplit: Array[String],
                     startPageCount: Long,
                     pageSplitCountMap: collection.Map[String, Long]) = {

    var lastPageCount = startPageCount.toDouble
    val pageSplitRatio = new mutable.HashMap[String, Double]()
    for (pageSplit <- targetPageSplit) {
      val currentPageSplitCount = pageSplitCountMap.get(pageSplit).get.toDouble
      val ratio = currentPageSplitCount / lastPageCount
      pageSplitRatio.put(pageSplit, ratio)
      lastPageCount = currentPageSplitCount
    }
    //   不加mkString   : mutable.Iterable[String]
    // 加上mkString ：String
    val convertStr: String = pageSplitRatio.map {
      case (pageSplit, ratio) => pageSplit + "=" + ratio
    }.mkString("|")
    //导入数据库
    val pageSplit = PageSplitConvertRate(taskUUID, convertStr)
    //转化为 rdd，DF导入数据库
    val pageSplitRationRDD: RDD[PageSplitConvertRate] = sparkSession.sparkContext.makeRDD(Array(pageSplit))
    import sparkSession.implicits._
    pageSplitRationRDD.toDF().write.format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "page_split_convert_rate_0204")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

  }

  def getUserVisitAction(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    val sql = "select * from user_visit_action where date >= '" + startDate + "' and date <= '" + endDate + "'"
    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd.map(item => (item.session_id, item))
  }

}
