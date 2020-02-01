import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class SessionAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {
  //要维护的结构
  val countMap = new mutable.HashMap[String, Int]()

  override def isZero: Boolean = {
    countMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val acc = new SessionAccumulator
    acc.countMap ++= this.countMap
    acc
  }

  override def reset(): Unit = {
    countMap.clear()
  }

  override def add(v: String): Unit = {
    if (!this.countMap.contains(v))
      //往map 中插入元素
      this.countMap += (v -> 0)
    this.countMap.update(v,countMap(v)+1)
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
        //确定是同一类型的累加器才合并，其他类型的累加器忽略
      case acc:SessionAccumulator => acc.countMap.foldLeft(this.countMap){
        case (map,(k,v))=> map += (k->(map.getOrElse(k,0)+v))
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = {
    this.countMap
  }
}
