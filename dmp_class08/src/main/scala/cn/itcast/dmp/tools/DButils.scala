package cn.itcast.dmp.tools

import java.util

import org.apache.kudu.Schema
import org.apache.kudu.client.{CreateTableOptions, KuduClient}
import org.apache.spark.sql.DataFrame

import org.apache.kudu.spark.kudu._

//保存结果数据到kudu中进行存储
object DButils {
  def saveData2Kudu(
                   dataFrame: DataFrame,
                   kuduMaster:String,
                   tableName:String,
                   schema:Schema,
                   partitionID:String
                   ): Unit ={

    //1、构建KuduClient对象
        val kuduClientBuilder = new KuduClient.KuduClientBuilder(kuduMaster)
        kuduClientBuilder.defaultSocketReadTimeoutMs(10000)
        val kuduClient: KuduClient = kuduClientBuilder.build()

    //2、创建表
      if(!kuduClient.tableExists(tableName)){
          //配置表的属性
          val options = new CreateTableOptions
          val partitions = new util.ArrayList[String]()
           //指定分区字段是什么
           partitions.add(partitionID)
           options.addHashPartitions(partitions,6)
           //设置副本数
           options.setNumReplicas(3)

        //开始创建
        kuduClient.createTable(tableName,schema,options)
      }

    //把dataFrame结果数据写入到kudu表中
    dataFrame.write
             .mode("append")
             .option("kudu.master" , kuduMaster)
             .option("kudu.table",tableName)
      //需要隐式转换 import org.apache.kudu.spark.kudu._
             .kudu


  }
}
