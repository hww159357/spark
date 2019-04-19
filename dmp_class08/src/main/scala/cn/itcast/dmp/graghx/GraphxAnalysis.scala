package cn.itcast.dmp.graghx

import java.util

import cn.itcast.dmp.tags.UserUnqueTags
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object GraphxAnalysis {

  //实现用户的统一识别
                 //   (userid,(用户的所有标识,用户的标签))
  def graph(tags: RDD[(String, (List[(String, Int)], List[(String, Double)]))], rowRDD: RDD[Row]) = {
     //构建点集合
   val vertexRDD: RDD[(Long, (List[(String, Int)], List[(String, Double)]))] = tags.mapPartitions(iter => {
     iter.map(line => {
       (line._1.hashCode.toLong, line._2)
     })
   })


  //构建边集合
     val edgeRDD: RDD[Edge[Int]] = rowRDD.map(line => {
       val userids: util.LinkedList[String] = UserUnqueTags.makeTags(line)
       //这里由于有很多字段信息，有些是没有，可以把list集合中第一个不为空的信息看成是userid
       val userid: String = userids.getFirst
       import scala.collection.JavaConverters._

       //构建边对象
       Edge(userid.hashCode.toLong, userids.asScala.toList.toString().hashCode.toLong, 0)

     })

    //构建图计算
        val graph = Graph(vertexRDD,edgeRDD)
     //构建连通图        (userid.hashcode.toLong,顶点)
       val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
                   // (userid.hashcode.toLong,顶点)  join  (userid.hashcode.toLong, (List[(String, Int)], List[(String, Double)])
       val join: RDD[(VertexId, (VertexId, (List[(String, Int)], List[(String, Double)])))] = vertices.join(vertexRDD)

      val result: RDD[(VertexId, (List[(String, Int)], List[(String, Double)]))] = join.map {
       case (userid, (aggrid, (alluserids, tags))) => (aggrid, (alluserids, tags))

     }
     result

  }

}
