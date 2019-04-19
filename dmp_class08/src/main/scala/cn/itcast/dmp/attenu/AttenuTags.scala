package cn.itcast.dmp.attenu

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

//todo:数据标签权重的衰减
object AttenuTags {

  def attenu(lastDataRDD: RDD[Row],coeff: Double):RDD[(String, (List[(String, Int)], List[(String, Double)]))]={
    lastDataRDD.map(row=>{

      //获取用户的所有标识
      // (MAC:52:54:00:e5:4d:ec,0)(OPENUDID:WMTBESLOKGWFGBWYIYZIDNXZQJRFFZYDHPHMCZAC,0)
      val userids: String = row.getAs[String]("userids")
      //MAC:52:54:00:e5:4d:ec,0)(OPENUDID:WMTBESLOKGWFGBWYIYZIDNXZQJRFFZYDHPHMCZAC,0
       val userids_tmp: String = userids.substring(1,userids.length-1)
      //MAC:52:54:00:e5:4d:ec,0 OPENUDID:WMTBESLOKGWFGBWYIYZIDNXZQJRFFZYDHPHMCZAC,0
      val useridsArray: Array[String] = userids_tmp.split("\\)\\(")

      var userid=useridsArray(0).split(",")(0)
      val useridsList: List[(String, Int)] = useridsArray.map(line => {
        val kv: Array[String] = line.split(",")
        val k: String = kv(0)
        val v: Int = kv(1).toInt
        (k, v)
      }).toList

      //获取用户所有的标签信息
       //(SEX@男,1.0)(K@视觉中国,1.0)(D00020002,1.0)
      val tags: String = row.getAs[String]("tags")
      //SEX@男,1.0)(K@视觉中国,1.0)(D00020002,1.0
      val tags_tmp: String = tags.substring(1,tags.length-1)
     // SEX@男,1.0 K@视觉中国,1.0 D00020002,1.0
      val tagsArray: Array[String] = tags_tmp.split("\\)\\(")
      val attenuTags: List[(String, Double)] = tagsArray.map(line => {
        val kv: Array[String] = line.split(",")
        val k = kv(0)
        val v: Double = kv(1).toDouble * coeff
        (k, v)
      }).toList

      //(String, (List[(String, Int)], List[(String, Double)
      (userid,(useridsList,attenuTags))
    })

  }


}
