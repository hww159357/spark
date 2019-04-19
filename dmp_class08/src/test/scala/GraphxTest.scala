import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GraphxTest{
  def main(args: Array[String]): Unit = {
      val sparkConf: SparkConf = new SparkConf().setAppName("GraphxTest").setMaster("local[2]")
      val sc = new SparkContext(sparkConf)
      sc.setLogLevel("warn")

    //图计算 Graph(V，E)
    //准备点集合
    val vertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(List(
      (1, ("张三", 18)),
      (2, ("李四", 19)),
      (3, ("王五", 20)),
      (4, ("赵六", 21)),
      (5, ("韩梅梅", 22)),
      (6, ("李雷", 23)),
      (7, ("小明", 24)),
      (9, ("tom", 25)),
      (10, ("jerry", 26)),
      (11, ("ession", 27))
    ))

    //准备边集合
    val edgesRDD: RDD[Edge[Int]] = sc.parallelize(List(
      Edge(1, 136, 0),
      Edge(2, 136, 0),
      Edge(3, 136, 0),
      Edge(4, 136, 0),
      Edge(5, 136, 0),
      Edge(4, 158, 0),
      Edge(5, 158, 0),
      Edge(6, 158, 0),
      Edge(7, 158, 0),
      Edge(9, 177, 0),
      Edge(10, 177, 0),
      Edge(11, 177, 0)
    ))

     //构建图计算  ------->   Graph(点，边)
     val graph = Graph(vertexRDD,edgesRDD)

    //获取所有的点对象
    graph.vertices.foreach(println)
    println("点的数量："+graph.numVertices)
    println("边的数量："+graph.numEdges)

    //构建连通图   (userid,aggrid)
     val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
     //获取所有的对象关系
    vertices.foreach(println)

    //1:List((zhangsan,lisi,wangwu))
    //vertices=(userid,aggrid)  join  vertexRDD=(VertexId, (String, Int)) ----->(userid,(aggrid,(String, Int))
     val joinRDD: RDD[(VertexId, (VertexId, (String, Int)))] = vertices.join(vertexRDD)
     joinRDD.foreach(println)

    val result: RDD[(VertexId, List[(String, Int)])] = joinRDD.map {
      case (userid, (aggrid, (name, age))) => (aggrid, List((name, age)))
    }
    result.foreach(println)

    result.reduceByKey(_ ++ _).foreach(println)

    sc.stop()

  }
}
