package cn.itcast.dmp.etl

import cn.itcast.dmp.`trait`.ProcessData
import cn.itcast.dmp.tools._
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.{DataFrame, SparkSession}

object AdAppAnalysis  extends ProcessData{
  private val kuduMaster: String = GlobalConfigUtils.kuduMaster
  private val sourceTable: String = GlobalConfigUtils.odsPrefix+DateUtils.getNowDate()
  private val destTable="adAppAnalysis"

  val kuduOptions=Map(
    "kudu.master" -> kuduMaster,
    "kudu.table" ->sourceTable
  )

  override def process(sparkSession: SparkSession, kuduContext: KuduContext): Unit = {
    //1、读取原始ods层表的数据
    val odsDF: DataFrame = sparkSession.read.options(kuduOptions).kudu
    odsDF.createTempView("ods")

    //2、数据处理分析
    val adAppAnalysis: DataFrame = sparkSession.sql(ContantsSQL.appAnalysis_temp)

    adAppAnalysis.createTempView("adAppAnalysis")
    val finalResult: DataFrame = sparkSession.sql(ContantsSQL.appAnalysis)

    //3、把结果数据保存到kudu中
    val schema: Schema = ContantsSchema.appAnalysisSchema
    //分区id
    val partitionID="appid"
    DButils.saveData2Kudu(finalResult,kuduMaster,destTable,schema,partitionID)


  }
}
