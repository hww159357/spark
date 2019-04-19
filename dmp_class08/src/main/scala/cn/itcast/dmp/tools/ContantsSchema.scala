package cn.itcast.dmp.tools

import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{Schema, Type}

import scala.collection.JavaConverters._
//后期整个工程的所有表的schema信息统一在这里编写
object ContantsSchema {
  //1、ods表的schema
  lazy val odsSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("ip", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("sessionid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("advertisersid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adorderid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adcreativeid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adplatformproviderid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("sdkversion", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("adplatformkey", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("putinmodeltype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("requestmode", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adppprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("requestdate", Type.STRING).nullable(true).build(),
      new ColumnSchemaBuilder("appid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("appname", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("uuid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("device", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("client", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("osversion", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("density", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("pw", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("ph", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("long", Type.STRING).nullable(false).build(), //TODO
      new ColumnSchemaBuilder("lat", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("provincename", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("cityname", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("ispid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("ispname", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("networkmannerid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("networkmannername", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("iseffective", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("isbilling", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adspacetype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adspacetypename", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("devicetype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("processnode", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("apptype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("district", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("paymode", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("isbid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("winprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("iswin", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("cur", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rate", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("cnywinprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("imei", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("mac", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("idfa", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("openudid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("androidid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rtbprovince", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rtbcity", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rtbdistrict", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rtbstreet", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("storeurl", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("realip", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("isqualityapp", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidfloor", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("aw", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("ah", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("imeimd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("macmd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("idfamd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("openudidmd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("androididmd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("imeisha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("macsha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("idfasha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("openudidsha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("androididsha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("uuidunknow", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("userid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("iptype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("initbidprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adpayment", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("agentrate", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("lomarkrate", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adxrate", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("title", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("keywords", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("tagid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("callbackdate", Type.STRING).nullable(true).build(),
      new ColumnSchemaBuilder("channelid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("mediatype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("email", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("tel", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("sex", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("age", Type.STRING).nullable(false).build()
    ).asJava
    new Schema(columns)
  }


  //2、编写基于地域数量分布情况的表schema信息
  lazy val proessRegionSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("provincename", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("cityname", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("num", Type.INT64).nullable(false).build()
    ).asJava
    new Schema(columns)

  }

  //3、编写广告地域数量分布情况的表schema信息
  lazy val adLocationAnalysisSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("provincename", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("cityname", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("originalRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("validRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsus", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsusRat", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adImpressions", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adClicks", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adClicksRat", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adCost", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adConsumption", Type.DOUBLE).nullable(false).build()
    ).asJava
    new Schema(columns)

  }

  //4、广告投放app分布情况的schema
  lazy val appAnalysisSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("appid", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("appname", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("originalRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("validRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsSus", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsSusRat", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("mediumDisplayNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediumClickNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediumClickRat", Type.DOUBLE).nullable(false).build()
    ).asJava
    new Schema(columns)
  }


  //5、广告投放手机设备分布情况的schema
  lazy val deviceAnalysisSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("client", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("device", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("originalRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("validRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsSus", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsSusRat", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("mediumDisplayNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediumClickNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediumClickRat", Type.DOUBLE).nullable(false).build()
    ).asJava
    new Schema(columns)
  }


  //6、广告投放网络类型分布情况的schema
  lazy val networkAnalysisSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("networkmannerid", Type.INT64).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("networkmannername", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("originalRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("validRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsSus", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsSusRat", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("mediumDisplayNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediumClickNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediumClickRat", Type.DOUBLE).nullable(false).build()
    ).asJava
    new Schema(columns)
  }

  //7、广告投放网络运营商分布情况的schema
  lazy val ispAnalysisSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("ispname", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("originalRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("validRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsSus", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsSusRat", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("mediumDisplayNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediumClickNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediumClickRat", Type.DOUBLE).nullable(false).build()
    ).asJava
    new Schema(columns)
  }
  //8、广告投放渠道分布情况的schema
  lazy val channelAnalysisSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("channelid", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("originalRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("validRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsSus", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsSusRat", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("mediumDisplayNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediumClickNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediumClickRat", Type.DOUBLE).nullable(false).build()
    ).asJava
    new Schema(columns)
  }

  //9、配置商圈信息表的schema
  lazy val tradeSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("geoHashCode", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("trade", Type.STRING).nullable(false).build()).asJava
    new Schema(columns)
  }

  //10、保持标签数据到kudu表的schema
  lazy val tagsSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("userids", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("tags", Type.STRING).nullable(false).build()).asJava
    new Schema(columns)
  }

}
