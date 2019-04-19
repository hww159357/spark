package cn.itcast.dmp.etl

import cn.itcast.dmp.`trait`.ProcessData
import cn.itcast.dmp.bean.IpRegion
import cn.itcast.dmp.tools._
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

//todo：解析数据文件，获取得到ip对应的经度、维度、省、市
object ImproveData extends ProcessData{
  private val path: String = GlobalConfigUtils.getDataPath
  //kudumaster地址
  private val kuduMaster: String = GlobalConfigUtils.kuduMaster
  //  ODS+日期------> ODS20190302
  private val tableName: String = GlobalConfigUtils.odsPrefix+DateUtils.getNowDate()

  override def process(sparkSession: SparkSession, kuduContext: KuduContext): Unit = {
    //1、加载json数据文件
       val jsonDF: DataFrame = sparkSession.read.format("json").load(path)
       val ipDF: DataFrame = jsonDF.select("ip")

    //2、解析ip信息
        val rowRDD: RDD[Row] = ipDF.rdd
    // 方法(rowRDD)------>RDD[IpRegion(ip，经度、维度、省、市)]
       val ipRegionRDD: RDD[IpRegion] = ParseIp.parseIp2Bean(rowRDD)
       //导入隐式转换
      import sparkSession.implicits._
      val ipRegionDF: DataFrame = ipRegionRDD.toDF
        ipRegionDF.show(10)

    //把这里的jsonDF和ipRegionDF注册成2张表 后期进行关联操作获取内容
    jsonDF.createTempView("ods")
    ipRegionDF.createTempView("ipRegion")

    //通过sparksql去关联2张表的数据
     val result: DataFrame = sparkSession.sql(ContantsSQL.odssql)

    //3、把结果数据保存到kudu表中
     val schema: Schema = ContantsSchema.odsSchema

      //指定表的分区字段
      val partitionID="ip"

     DButils.saveData2Kudu(result,kuduMaster,tableName,schema,partitionID)

  }
}
