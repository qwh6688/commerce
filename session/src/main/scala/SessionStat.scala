import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils, StringUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SessionStat {


  def main(args: Array[String]): Unit = {
    val jsonStr: String = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam: JSONObject = JSONObject.fromObject(jsonStr)
    val taskUUID: String = UUID.randomUUID().toString
    //println(taskUUID) ab0d1371-971d-4f75-aaee-24772a2902d4
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SessionStat")
    //创建sparkSession（包含sparkContext）
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val actionRDD: RDD[UserVisitAction] = getOrActionRDD(sparkSession, taskParam)
    //     actionRDD.foreach(println)
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = actionRDD.map(item => (item.session_id, item))
    val session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey
    session2GroupActionRDD.cache()
    //    session2GroupActionRDD.foreach(println)
    val userId2AggrInfoRDD: RDD[(Long, String)] = getSessionFullInfo(sparkSession, session2GroupActionRDD)
    userId2AggrInfoRDD.foreach(println)


  }

  def getSessionFullInfo(sparkSession: SparkSession, session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])])= {

    // userId2AggrInfoRDD: RDD[(userId, aggrInfo)]
    val userId2AggrInfoRDD: RDD[(Long, String)] = session2GroupActionRDD.map {
      case (sessionId, iterableAction) => {
        var userId = -1L
        var startTime: Date = null
        var endTime: Date = null
        var stepLength = 0
        val searchKeywords = new StringBuffer("")
        val clickCategories = new StringBuffer("")
        for (action <- iterableAction) {
          if (userId == -1L) {
            userId = action.user_id
          }
          val actionTime: Date = DateUtils.parseTime(action.action_time)
          if (startTime == null || startTime.after(actionTime)) {
            startTime = actionTime
          }
          if (endTime == null || endTime.before(actionTime)) {
            endTime = actionTime
          }

          val searchKeyword = action.search_keyword
          if (StringUtils.isNotEmpty(searchKeyword) && !searchKeywords.toString.contains(searchKeyword)) {
            searchKeywords.append(searchKeyword + ",")

          }
          val clickCategoryId = action.click_category_id
          if (clickCategoryId != -1L && clickCategories.toString.contains(clickCategoryId)) {
            clickCategories.append(clickCategoryId + ",")
          }
          stepLength += 1
        }

        val searchKw = StringUtils.trimComma(searchKeywords.toString)
        val clickCg = StringUtils.trimComma(clickCategories.toString)

        val visitLength = (endTime.getTime - startTime.getTime) / 1000
        val aggInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)
        (userId, aggInfo)
      }
    }
    userId2AggrInfoRDD
}

  def getOrActionRDD(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    val sql = "select * from user_visit_action where date>='" + startDate + "' and date <= '" + endDate + "'"
    //DF ==》DS 需要导入隐式转换bao
    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }
}
