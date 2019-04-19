package cn.itcast.dmp.trade

import ch.hsr.geohash.GeoHash
import cn.itcast.dmp.`trait`.ProcessData
import cn.itcast.dmp.bean.BTrade
import cn.itcast.dmp.tools._
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object ProcessTrade  extends ProcessData{
  private val kuduMaster: String = GlobalConfigUtils.kuduMaster
                                       //ODS20190304
  private val sourceTable: String = GlobalConfigUtils.odsPrefix+DateUtils.getNowDate()

  private val destTableName="businessTrade"

  //配置kudu表的信息
  val kuduOptions=Map(
    "kudu.master" -> kuduMaster,
    "kudu.table" -> sourceTable
  )

  override def process(sparkSession: SparkSession, kuduContext: KuduContext): Unit = {
     //1、获取ods层表的数据
         val odsDF: DataFrame = sparkSession.read.options(kuduOptions).kudu

         //过滤出过国内的ip信息    国内的经纬度范围   73 <经度< 136,     3<维度<54
         odsDF.createTempView("ods")
         val chinaDF: DataFrame = sparkSession.sql(ContantsSQL.filter_non_china)
         val rowRDD: RDD[Row] = chinaDF.rdd

    //2、处理数据得到商圈信息
    val result: RDD[BTrade] = rowRDD.map(row => {
      //经度
      val longitude: Double = row.getAs[String]("long").toDouble
      //维度
      val latitude: Double = row.getAs[String]("lat").toDouble

      //生成表的主键标识
      val geoHashCode: String = GeoHash.withCharacterPrecision(latitude, longitude, 8).toBase32

      //https://restapi.amap.com/v3/geocode/regeo?output=xml&location=116.310003,39.991957&key=1c9d3317451726a6d345b3e00d0c46de
      val location = longitude + "," + latitude

      //每一个经度和维度的商圈信息
      val trade: String = ExtraTrade.parseLocation(location)

      //封装返回的结果数据
      BTrade(geoHashCode, trade)
    }).filter(x => !"".equals(x.trade))

    //3、保存商圈信息到kudu表中
    import  sparkSession.implicits._
    val tradeDF: DataFrame = result.toDF

    val schema: Schema = ContantsSchema.tradeSchema
    val partitionID="geoHashCode"
    DButils.saveData2Kudu(tradeDF,kuduMaster,destTableName,schema,partitionID)

  }
}
