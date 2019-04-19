package cn.itcast.dmp.merge

import cn.itcast.dmp.aggr.AggrTags
import cn.itcast.dmp.attenu.AttenuTags
import cn.itcast.dmp.graghx.GraphxAnalysis
import cn.itcast.dmp.tools.{ContantsSchema, DButils, DateUtils, GlobalConfigUtils}
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

//todo:把历史标签的数据和今天的标签数据进行合并
object MergeTags {

  //遇到以下这些问题
  //1、历史标签数据的权重衰减------>牛顿冷却系数数据模型 f(t)=初始温度*etx(-冷却系数*时间间隔)
  //2、最终的标签结果数据：历史的标签数据衰减之后 + 今天的标签数据权重
        //最终的数据标签权重=历史标签的权重*衰减系数+今天的数据标签权重
  //4、用户的标签数据合并  ------>历史标签数据与今天的标签数据合并
  //5、用户的统一识别  -------》spark Graphx图计算
  //6、用户的标签数据聚合 -------->就是把相同用户出现的标签进行权重累加


  private val coeff: Double = GlobalConfigUtils.getTagsCoeff.toDouble
  def merge(todayRDD: RDD[Row], lastDataRDD: RDD[Row], rowRDD: RDD[Row], sparkSession: SparkSession, kuduContext: KuduContext): Unit ={
      //1、历史标签数据的衰减
    val attenuRDD: RDD[(String, (List[(String, Int)], List[(String, Double)]))] = AttenuTags.attenu(lastDataRDD,coeff)

      //2、历史标签数据与今天的标签数据合并   union
    val todayDataRDD: RDD[(String, (List[(String, Int)], List[(String, Double)]))] = operatorTodayData(todayRDD)
    val  allTagsRDD: RDD[(String, (List[(String, Int)], List[(String, Double)]))] = attenuRDD.union(todayDataRDD)

      //3、使用spark的图计算做统一的用户识别
    val graphRDD: RDD[(VertexId, (List[(String, Int)], List[(String, Double)]))] = GraphxAnalysis.graph(allTagsRDD,rowRDD)

      //4、标签的聚合
    val aggrRDD: RDD[(VertexId, (List[(String, Int)], List[(String, Double)]))] = AggrTags.aggr(graphRDD)


      //5、保持结果数据到kudu表中
    val result: RDD[(String, String)] = aggrRDD.map {

      case (userid, (userids, tags)) => (userids.mkString, tags.mkString)

    }

    import sparkSession.implicits._
    val allTagsDF: DataFrame = result.toDF("userids","tags")

    val kuduMaster: String = GlobalConfigUtils.kuduMaster
       //tags.prefix="TP"
    val tableName: String = GlobalConfigUtils.getTagsPrefix+DateUtils.getNowDate()
    val schema: Schema = ContantsSchema.tagsSchema
    val partitionID="userids"
    DButils.saveData2Kudu(allTagsDF,kuduMaster,tableName,schema,partitionID)

  }


  //操作今天的标签数据结果，让其数据格式与历史标签数据衰减之后的类型保持一致
  def operatorTodayData(todayRDD: RDD[Row]):RDD[(String, (List[(String, Int)], List[(String, Double)]))]={
    todayRDD.map(row=>{

      //获取用户的所有标识
      // (MAC:52:54:00:e5:4d:ec,0)(OPENUDID:WMTBESLOKGWFGBWYIYZIDNXZQJRFFZYDHPHMCZAC,0)
      val userids: String = row.getAs[String]("userids")
      //MAC:52:54:00:e5:4d:ec,0)(OPENUDID:WMTBESLOKGWFGBWYIYZIDNXZQJRFFZYDHPHMCZAC,0
      val userids_tmp: String = userids.substring(1,userids.length-1)
      //MAC:52:54:00:e5:4d:ec,0 OPENUDID:WMTBESLOKGWFGBWYIYZIDNXZQJRFFZYDHPHMCZAC,0
      val useridsArray: Array[String] = userids_tmp.split("\\)\\(")

      var userid=useridsArray(0).split(",")(0)
      val useridsList: List[(String, Int)] = useridsArray.map(line => {
        val kv: Array[String] = line.split(",")
        val k: String = kv(0)
        val v: Int = kv(1).toInt
        (k, v)
      }).toList

      //获取用户所有的标签信息
      //(SEX@男,1.0)(K@视觉中国,1.0)(D00020002,1.0)
      val tags: String = row.getAs[String]("tags")
      //SEX@男,1.0)(K@视觉中国,1.0)(D00020002,1.0
      val tags_tmp: String = tags.substring(1,tags.length-1)
      // SEX@男,1.0 K@视觉中国,1.0 D00020002,1.0
      val tagsArray: Array[String] = tags_tmp.split("\\)\\(")
      val attenuTags: List[(String, Double)] = tagsArray.map(line => {
        val kv: Array[String] = line.split(",")
        val k = kv(0)
        val v: Double = kv(1).toDouble
        (k, v)
      }).toList

      //(String, (List[(String, Int)], List[(String, Double)
      (userid,(useridsList,attenuTags))
    })

  }



}
