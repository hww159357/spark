package cn.itcast.dmp

import cn.itcast.dmp.es.Data2ES
import cn.itcast.dmp.etl._
import cn.itcast.dmp.tags.DataTags
import cn.itcast.dmp.tools.GlobalConfigUtils
import cn.itcast.dmp.trade.ProcessTrade
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

//todo:通过spark开发不同的业务逻辑
object DmpMain {

    //定义kuduMaster地址
   private val kuduMaster: String = GlobalConfigUtils.kuduMaster
  def main(args: Array[String]): Unit = {
     //1、构建sparkConf对象，设置一些属性
    val sparkConf: SparkConf = new SparkConf().setAppName("DmpMain")
                              .setMaster("local[6]")
                              .set("spark.worker.timeout", GlobalConfigUtils.sparkWorkerTimeOut)
                              .set("spark.rpc.askTimeout", GlobalConfigUtils.sparkRpcAskTimeOut)
                              .set("spark.network.timeout", GlobalConfigUtils.sparkNetworkTimeOut)
                              .set("spark.cores.max", GlobalConfigUtils.sparkCoresMax)
                              .set("spark.task.maxFailures", GlobalConfigUtils.sparkTaskMaxFailures)
                              .set("spark.speculation", GlobalConfigUtils.sparkSpeculation)
                              .set("spark.driver.allowMutilpleContext", GlobalConfigUtils.sparkDriverAllowMutilpleContext)
                              .set("spark.serializer", GlobalConfigUtils.sparkSerializer)
                              .set("spark.buffer.pageSize", GlobalConfigUtils.sparkBufferPageSize)
                              .set("cluster.name",GlobalConfigUtils.getClusterName)
                              .set("es.nodes",GlobalConfigUtils.getESNodes)
    //2、创建SparkSession对象
        val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //3、获取sparkContext
       val sc: SparkContext = sparkSession.sparkContext
       sc.setLogLevel("warn")

    //4、创建KuduContext对象
      val kuduContext = new KuduContext(kuduMaster,sc)

    //todo: 5、统计需求指标
      //todo: 5.1 基于ip地址解析经度、维度、省、市  最后生成ods层表的数据 存储在kudu中
       ImproveData.process(sparkSession,kuduContext)

       //todo: 5.2 统计各个地域数量分布情况
       ProcessRegion.process(sparkSession,kuduContext)

        //todo:5.3 统计广告地域分布情况
        //AdLocationAnylysis.process(sparkSession,kuduContext)

       //todo:5.4 统计广告投放的APP分布情况
        // AdAppAnalysis.process(sparkSession,kuduContext)

      //todo:5.5 统计广告投放的手机设备类型分布情况
       //AdDeviceAnalysis.process(sparkSession,kuduContext)

      //todo:5.6 统计广告投放的网络类型分布情况
        AdNetworkAnalysis.process(sparkSession,kuduContext)

     //todo:5.7 统计广告投放的网络运营商分布情况
       //AdIspAnalysis.process(sparkSession,kuduContext)

     //todo:5.8 统计广告投放的渠道分布情况
       AdChannelAnalysis.process(sparkSession,kuduContext)

    //todo：5.9 生成商圈库
      ProcessTrade.process(sparkSession,kuduContext)

    //todo:5.10 数据标签化
      DataTags.process(sparkSession,kuduContext)

    //todo：5.11 数据落地到ES
    Data2ES.process(sparkSession,kuduContext)



    //关闭。
    if(!sc.isStopped){
      sc.stop()
    }

  }
}
