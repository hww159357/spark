package cn.itcast.dmp.etl

import cn.itcast.dmp.`trait`.ProcessData
import cn.itcast.dmp.tools._
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.{DataFrame, SparkSession}

//todo:统计每一个地域（省市）数量分布情况
object ProcessRegion extends ProcessData{
    private val kuduMaster: String = GlobalConfigUtils.kuduMaster
                                        //ODS20190302
    private val tableName: String = GlobalConfigUtils.odsPrefix+DateUtils.getNowDate()

    //目标表
    val sinkTable="processRegion"

    //构建map集合，封装kudu集群相关信息
    val kuduOptions=Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> tableName
    )

  override def process(sparkSession: SparkSession, kuduContext: KuduContext): Unit = {
       //1、加载ods层表的数据
       val odsDF: DataFrame = sparkSession.read.options(kuduOptions).kudu
        odsDF.createTempView("ods")

      //2、数据的处理分析
       val result: DataFrame = sparkSession.sql(ContantsSQL.regionSql)

     //3、保存数据到kudu中
       val schema: Schema = ContantsSchema.proessRegionSchema
      //表的分区字段
       val partitionID="provincename"

    DButils.saveData2Kudu(result,kuduMaster,sinkTable,schema,partitionID)

  }
}
