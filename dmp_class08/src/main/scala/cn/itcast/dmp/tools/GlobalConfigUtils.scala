package cn.itcast.dmp.tools

import com.typesafe.config.{Config, ConfigFactory}

//构建读取配置文件的工具类
object GlobalConfigUtils {
     //加载application.conf文件信息
  private val config: Config = ConfigFactory.load()

  def sparkWorkerTimeOut=config.getString("spark.worker.timeout")
  def sparkRpcAskTimeOut=config.getString("spark.rpc.askTimeout")
  def sparkNetworkTimeOut=config.getString("spark.network.timeout")
  def sparkCoresMax=config.getString("spark.cores.max")
  def sparkTaskMaxFailures=config.getString("spark.task.maxFailures")
  def sparkSpeculation=config.getString("spark.speculation")
  def sparkDriverAllowMutilpleContext=config.getString("spark.driver.allowMutilpleContext")
  def sparkSerializer=config.getString("spark.serializer")
  def sparkBufferPageSize=config.getString("spark.buffer.pageSize")

  def kuduMaster=config.getString("kudu.master")

  //获取文件
  def getDataPath=config.getString("data.path")

  def geoLiteCityDat=config.getString("GeoLiteCity.dat")

  def getQqwry=config.getString("qqwrydat"

  def qqwryDatDir=config.getString("qqwry.dat.dir")
  )
  //指定表的前缀
  def odsPrefix=config.getString("ods.prefix")

  //获取高德地图的key
  def getKey=config.getString("key")

  //获取字段文件的路径
  def appIDName=config.getString("appID_name")
  def devicedic=config.getString("devicedic")

  //获取impala的url
  def getUrl=config.getString("url")



  //获取标签数据表的前缀
  def getTagsPrefix=config.getString("tags.prefix")


  //获取衰减系数
  def getTagsCoeff=config.getString("tags.coeff")

  def getClusterName=config.getString("cluster.name")
  def getESNodes=config.getString("esNodes")


}
