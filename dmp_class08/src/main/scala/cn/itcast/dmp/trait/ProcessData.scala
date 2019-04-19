package cn.itcast.dmp.`trait`

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession

//处理数据的接口类
trait ProcessData {
  def process(sparkSession:SparkSession,kuduContext: KuduContext)
}
