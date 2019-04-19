package cn.itcast.dmp.tags

import java.util

import cn.itcast.dmp.`trait`.ProcessData
import cn.itcast.dmp.aggr.AggrTags
import cn.itcast.dmp.graghx.GraphxAnalysis
import cn.itcast.dmp.merge.MergeTags
import cn.itcast.dmp.tools._
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._

object DataTags extends ProcessData{
  /**
    * 1、原始数据做数据清洗
    * 2、给用户打标签（年龄、性别、广告位类型标签、app名称、地域、商圈、设备（网络类型、操作系统、网络运营商）、用户的唯一标识、关键字等）
    * 3、用户标签衰减
    * 4、用户的标识重复
    */
    private val kuduMaster: String = GlobalConfigUtils.kuduMaster
    private val sourceTable: String = GlobalConfigUtils.odsPrefix+DateUtils.getNowDate()
    //获取字典文件路径
    private val appIDName: String = GlobalConfigUtils.appIDName
     private val devicedic: String = GlobalConfigUtils.devicedic

   //获取标签的历史表的名称
    private val yesterdayTagsTable: String = GlobalConfigUtils.getTagsPrefix+DateUtils.getYesterDay()


   //配置今天的ods数据
   val kuduOption=Map(
     "kudu.master" ->kuduMaster,
     "kudu.table" ->sourceTable
   )

  //配置昨天的标签数据
  val kuduhistoryOption=Map(
    "kudu.master" ->kuduMaster,
    "kudu.table" ->yesterdayTagsTable
  )


  override def process(sparkSession: SparkSession, kuduContext: KuduContext): Unit = {
     //1、加载数据
      val odsDF: DataFrame = sparkSession.read.options(kuduOption).kudu

     //2、过滤出有效的数据
        odsDF.createTempView("ods")
       val dataFrame: DataFrame = sparkSession.sql(ContantsSQL.non_empty_sql)
       val rowRDD: RDD[Row] = dataFrame.rdd

    //3、读取数据字典文件
      val appIDRDD: RDD[String] = sparkSession.sparkContext.textFile(appIDName)
      val appIdArray: Array[String] = appIDRDD.collect()

    //XRX100003##YY直播
    val appIDMap: Map[String, String] = appIdArray.map(x=>(x.split("##")(0),x.split("##")(1))).toMap

    //把数据广播出去
    val appIdMapBroadcast: Broadcast[Map[String, String]] = sparkSession.sparkContext.broadcast(appIDMap)


    val devicedicRDD: RDD[String] = sparkSession.sparkContext.textFile(devicedic)
    val arrayDevice: Array[String] = devicedicRDD.collect()
    //1##D00010001
    val devicedicMap: Map[String, String] = arrayDevice.map(x=>(x.split("##")(0),x.split("##")(1))).toMap

    //把数据广播出去
    val devicedicMapBroadcast: Broadcast[Map[String, String]] = sparkSession.sparkContext.broadcast(devicedicMap)

    //4、数据打标签的操作
    val tags: RDD[(String, (List[(String, Int)], List[(String, Double)]))] = rowRDD.map(line => {
      //4.1  广告位的类型标签
      val adTypeTags: Map[String, Double] = AdTypeTags.makeTags(line)

      //4.2 地域标签
      val areaTags: Map[String, Double] = AreaTags.makeTags(line)

      //4.3 app名称标签
      val appTags: Map[String, Double] = AppTags.makeTags(line, appIdMapBroadcast.value)

      //4.4 设备标签--（网络类型、操作系统、网络运营商）
      val deviceTags: Map[String, Double] = DeviceTags.makeTags(line, devicedicMapBroadcast.value)

      //4.5 性别标签
      val sexTags: Map[String, Double] = SexTags.makeTags(line)

      //4.6 年龄标签
      val ageTags: Map[String, Double] = AgeTags.makeTags(line)

      //4.7 关键字标签
      val keywordsTags: Map[String, Double] = KeyWordsTags.makeTags(line)

      //4.8 商圈标签
      val tradeTags: Map[String, Double] = TradeTags.makeTags(line)

      //4.9 用户标识标签
      val userids: util.LinkedList[String] = UserUnqueTags.makeTags(line)
      //这里由于有很多字段信息，有些是没有，可以把list集合中第一个不为空的信息看成是userid
      val userid: String = userids.getFirst

      //为了后期处理 起来方便
      val useridsList: List[(String, Int)] = userids.asScala.toList.map(x => (x, 0))

      val tags = adTypeTags ++ areaTags ++ appTags ++ deviceTags ++ sexTags ++ ageTags ++ keywordsTags ++ tradeTags

      //封装结果数据进行返回
      //用户的唯一标识，用户的所有标识信息，用户的标签信息
      (userid, (useridsList, tags.toList))

    })

     //使用spark Graphx来实现用户的统一识别
      val result: RDD[(VertexId, (List[(String, Int)], List[(String, Double)]))] = GraphxAnalysis.graph(tags,rowRDD)

    // 标签的聚合
    val aggrRDD: RDD[(VertexId, (List[(String, Int)], List[(String, Double)]))] = AggrTags.aggr(result)

    //保存数据结果
    //用户所有的标识，用户所有的标签信息
    val resultRDD: RDD[(String, String)] = aggrRDD.map {
      case (userid, (userids, tags)) => (userids.mkString, tags.mkString)
    }
    import sparkSession.implicits._
    val todayDF: DataFrame = resultRDD.toDF("userids","tags")
    val todayRDD: RDD[Row] = todayDF.rdd

    //获取历史的标签数据
     val yesterdayTagsDF: DataFrame = sparkSession.read.options(kuduhistoryOption).kudu
     val lastDataRDD: RDD[Row] = yesterdayTagsDF.rdd


    //历史的标签数据和今天的标签数据合并
      MergeTags.merge(todayRDD,lastDataRDD,rowRDD,sparkSession,kuduContext)


    /**
      * 下面的操作只是为了生成历史的标签数据，在真实企业中是不需要，这里这是为了做个测试
      */
//
//    val schema: Schema = ContantsSchema.tagsSchema
//    val partitionID="userids"
//    DButils.saveData2Kudu(todayDF,kuduMaster,yesterdayTagsTable,schema,partitionID)


    //resultRDD.repartition(1).saveAsTextFile("d:\\tags5")
  }
}
