package cn.itcast.dmp.tags

import cn.itcast.dmp.`trait`.Tags
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

object SexTags extends Tags{
  /**
    * 给用户打标签的方法
    *
    * @param args 它表示参数的个数和类型不规定
    * @return Map[String,Double]---->key：标签名称，value:标签权重
    */
  override def makeTags(args: Any*): Map[String, Double] = {
    var map= Map[String, Double]()

    if(args.length >0){
      val row: Row = args(0).asInstanceOf[Row]
      val sex: String = row.getAs[String]("sex")

      if(StringUtils.isNotBlank(sex)){
         sex match {
           case "0" => map +=("SEX@女" -> 1)
           case _   => map +=("SEX@男" -> 1)
         }
      }
    }

    map

  }
}
