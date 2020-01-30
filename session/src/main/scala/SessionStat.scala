import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.ParamUtils
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
    val actionRDD: RDD[UserVisitAction] = getOrActionRDD(sparkSession,taskParam)
//     actionRDD.foreach(println)
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = actionRDD.map(item=>(item.session_id,item))
    val session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey
    session2GroupActionRDD.cache()
    session2GroupActionRDD.foreach(println)



  }

  def getOrActionRDD(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate: String = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)
    val sql = "select * from user_visit_action where date>='" +startDate+ "' and date <= '"+endDate+"'"
    //DF ==》DS 需要导入隐式转换bao
    import  sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }
}
