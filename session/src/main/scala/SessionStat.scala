import java.util.{Date, UUID}

import net.sf.json.JSONObject
import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

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
    // actionRDD.foreach(println)
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = actionRDD.map(item => (item.session_id, item))
    val session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey
    session2GroupActionRDD.cache()
    //    session2GroupActionRDD.foreach(println)
    val sessionId2FullInfoRDD = getSessionFullInfo(sparkSession, session2GroupActionRDD)
    //    sessionId2FullInfoRDD.foreach(println) 累加器中维护的是 Map结构的数据
    val sessionStatisticAccumulator = new SessionAccumulator
    sparkSession.sparkContext.register(sessionStatisticAccumulator)
    //
    val sessionId2FilterRDD = getSessionFilteredRDD(taskParam, sessionId2FullInfoRDD, sessionStatisticAccumulator)
    /*
    不加上这一句相当与没有action算子，数据库中的数据不准确
     */
    sessionId2FilterRDD.foreach(println(_))
    getSessionRatio(sparkSession, taskUUID, sessionStatisticAccumulator.value)


  }

  def getSessionRatio(sparkSession: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {
    val session_count = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble

    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)


    //rdd==》DF 然后写入到mysql数据库
    val stat = SessionAggrStat(taskUUID, session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)
    val sessionRatioRDD: RDD[SessionAggrStat] = sparkSession.sparkContext.makeRDD(Array(stat))
    import sparkSession.implicits._
    sessionRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_stat_ratio_0201")
      .mode(SaveMode.Append)
      .save()


  }


  def calculateStepLength(stepLength: Long, sessionStatisticAccumulator: SessionAccumulator) = {
    if (stepLength >= 1 && stepLength <= 3) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_1_3)
    } else if (stepLength >= 4 && stepLength <= 6) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_30_60)
    } else if (stepLength > 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_60)
    }
  }


  def calculateVisitLength(visitLength: Long, sessionStatisticAccumulator: SessionAccumulator) = {
    if (visitLength >= 1 && visitLength <= 3) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    } else if (visitLength >= 4 && visitLength <= 6) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    } else if (visitLength >= 7 && visitLength <= 9) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    } else if (visitLength > 30 && visitLength <= 60) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    } else if (visitLength > 60 && visitLength <= 180) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLength > 180 && visitLength <= 600) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    } else if (visitLength > 600 && visitLength <= 1800) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    } else if (visitLength > 1800) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30m)
    }

  }

  def getSessionFilteredRDD(taskParam: JSONObject, sessionId2FullInfoRDD: RDD[(String, String)], sessionAccumulator: SessionAccumulator) = {
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var filterInfo = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")
    if (filterInfo.endsWith("\\|"))
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)
    sessionId2FullInfoRDD.filter {
      case (sessionId, fullInfo) => {
        var success = true
        if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {
          success = false
        } else if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CLICK_CATEGORY_IDS, filterInfo, Constants.PARAM_CATEGORY_IDS)) {
          success = false
        }
        if (success) {
          sessionAccumulator.add(Constants.SESSION_COUNT)

          val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong

          calculateVisitLength(visitLength, sessionAccumulator)
          calculateStepLength(stepLength, sessionAccumulator)
        }

        success
      }

    }


  }


  def getSessionFullInfo(sparkSession: SparkSession, session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])]) = {

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
        // searchKeywords.toString.substring(0, searchKeywords.toString.length)
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

    val sql = "select * from user_info"
    import sparkSession.implicits._
    // userId2InfoRDD:RDD[(userId, UserInfo)]
    val userId2InfoRDD: RDD[(Long, UserInfo)] = sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item))
    val sessionId2FullInfoRDD: RDD[(String, String)] = userId2AggrInfoRDD.join(userId2InfoRDD).map {
      case (userId, (aggInfo, userInfo)) => {
        val age = userInfo.age
        val professional = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city
        val fullInfo = aggInfo + "|" +
          Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city
        val sessionId = StringUtils.getFieldFromConcatString(aggInfo, "\\|", Constants.FIELD_SESSION_ID)
        (sessionId, fullInfo)
      }
    }
    sessionId2FullInfoRDD

  }


  def getOrActionRDD(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    val sql = "select * from user_visit_action where date>='" + startDate + "' and date <= '" + endDate + "'"
    //DF ==》DS 需要导入隐式转换bao
    import sparkSession.implicits._
    //  val rdd: RDD[UserVisitAction] =
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }

}


