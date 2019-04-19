package cn.itcast.dmp.tags

import cn.itcast.dmp.`trait`.Tags
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

object AppTags extends Tags{
  /**
    * 给用户打标签的方法
    *
    * @param args 它表示参数的个数和类型不规定
    * @return Map[String,Double]---->key：标签名称，value:标签权重
    */
  override def makeTags(args: Any*): Map[String, Double] = {
    var map= Map[String, Double]()

    if(args.length >1){
       val row: Row = args(0).asInstanceOf[Row]
       val appid: String = row.getAs[String]("appid")
       var appname: String = row.getAs[String]("appname")

      val appMap: Map[String, String] = args(1).asInstanceOf[Map[String, String]]

        if(StringUtils.isBlank(appname)){
          appname= appMap.getOrElse(appid,appname)
        }

       map +=("APP@"+appname -> 1)

    }

   map
  }
}
