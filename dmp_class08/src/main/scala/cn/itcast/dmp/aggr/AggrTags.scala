package cn.itcast.dmp.aggr

import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

//todo；标签聚合
object AggrTags {
  def aggr(result: RDD[(VertexId, (List[(String, Int)], List[(String, Double)]))]) = {

    val aggrTags: RDD[(VertexId, (List[(String, Int)], List[(String, Double)]))] = result.reduceByKey {
      case (before, after) => {
        val userids: List[(String, Int)] = before._1 ++ after._1
        val tags: List[(String, Double)] = before._2 ++ after._2

        //实现同一用户标签数据的权重累加
         //List[(K@宁静,1.0),(K@宁静,1.0),(APP@360手机卫士,1.0))]
                                      //List[(K@宁静,1.0),(K@宁静,1.0))
        val values: Map[String, Double] = tags.groupBy(_._1).mapValues(x=>x.foldLeft(0.0)((y,z)=>y+z._2))
        val aggrTags: List[(String, Double)] = values.toList
        (userids.distinct,aggrTags)

      }

    }
    aggrTags
  }


}
