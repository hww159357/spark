package cn.itcast.dmp.tools

import cn.itcast.dmp.bean.IpRegion
import cn.itcast.dmp.tools.ip.{IPAddressUtils, IPLocation}
import com.maxmind.geoip.{Location, LookupService}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

//解析ip地址
object ParseIp {
   private val geoLiteCityDat: String = GlobalConfigUtils.geoLiteCityDat

  def parseIp2Bean(rowRDD: RDD[Row]):RDD[IpRegion]={

     //把RDD[Row]---> RDD[String]  这里的String就是每一个ip地址的值
    val ipRDD: RDD[String] = rowRDD.map(row => {
      val ip: String = row.getAs[String]("ip")
      ip
    })

    ipRDD.mapPartitions(iter =>{
       val lookupService = new LookupService(geoLiteCityDat)
      // 获取每一个ip地址
      iter.map(ip=>{
        val location: Location = lookupService.getLocation(ip)
        //经度
        val longitude: String = location.longitude.toString
        //维度
        val latitude: String = location.latitude.toString

        //获取ip地址对应的省市
        val addressUtils = new IPAddressUtils
        val iPLocation: IPLocation = addressUtils.getregion(ip)

        //省
        val province: String = iPLocation.getRegion
        //市
        val city: String = iPLocation.getCity

        //返回结果
        IpRegion(ip,longitude,latitude,province,city)
      })

    })

  }
}
