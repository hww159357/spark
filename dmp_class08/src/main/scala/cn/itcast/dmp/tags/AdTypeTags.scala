package cn.itcast.dmp.tags

import cn.itcast.dmp.`trait`.Tags
import org.apache.spark.sql.Row

object AdTypeTags  extends Tags{
  /**
    * 给用户打标签的方法
    *
    * @param args 它表示参数的个数和类型不规定
    * @return Map[String,Double]---->key：标签名称，value:标签权重
    */
  override def makeTags(args: Any*): Map[String, Double] = {
    var map=Map[String,Double]()

    if(args.length>0){
      val row: Row = args(0).asInstanceOf[Row]
      val adspacetype: Long = row.getAs[Long]("adspacetype")

        //广告位类型（1：banner 2：插屏 3：全屏）
      adspacetype match {
        case x if x ==1 => map +=("PC@banner" -> 1 )
        case x if x ==2 => map +=("PC@插屏" -> 1 )
        case x if x ==3 => map +=("PC@全屏" -> 1 )
      }
    }
    map
  }
}
