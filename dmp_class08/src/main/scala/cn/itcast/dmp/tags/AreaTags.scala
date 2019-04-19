package cn.itcast.dmp.tags

import cn.itcast.dmp.`trait`.Tags
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

object AreaTags extends Tags{
  /**
    * 给用户打标签的方法
    *
    * @param args 它表示参数的个数和类型不规定
    * @return Map[String,Double]---->key：标签名称，value:标签权重
    */
  override def makeTags(args: Any*): Map[String, Double] = {
    var map= Map[String, Double]()
    if(args.length>0){
      val row: Row = args(0).asInstanceOf[Row]
       val provincename: String = row.getAs[String]("provincename")
       val cityname: String = row.getAs[String]("cityname")

       if(StringUtils.isNotBlank(provincename)){
            map +=("PZ@"+provincename ->1)
       }
      if(StringUtils.isNotBlank(cityname)){
        map +=("CZ@"+cityname ->1)
      }
    }

    map
  }
}
