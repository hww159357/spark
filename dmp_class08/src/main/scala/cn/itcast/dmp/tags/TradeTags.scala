package cn.itcast.dmp.tags

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import ch.hsr.geohash.GeoHash
import cn.itcast.dmp.`trait`.Tags
import cn.itcast.dmp.tools.GlobalConfigUtils
import org.apache.spark.sql.Row

object TradeTags extends Tags {
  val url = GlobalConfigUtils.getUrl

  /**
    * 给用户打标签的方法
    *
    * @param args 它表示参数的个数和类型不规定
    * @return Map[String,Double]---->key：标签名称，value:标签权重
    */
  override def makeTags(args: Any*): Map[String, Double] = {
    var map = Map[String, Double]()

    if (args.length > 0) {
      val row: Row = args(0).asInstanceOf[Row]
      //获取经度
      val longitude: String = row.getAs[String]("long")
      //维度
      val latitude: String = row.getAs[String]("lat")
      val geoHashcode: String = GeoHash.withCharacterPrecision(latitude.toDouble, longitude.toDouble, 8).toBase32


      //通过impala的api去查询结果数据
          var connection: Connection =null
          try {
            connection = DriverManager.getConnection(url)
            val sql = "select trade from businesstrade where geohashcode='" + geoHashcode + "'"
            val ps: PreparedStatement = connection.prepareStatement(sql)
            val resultSet: ResultSet = ps.executeQuery()

            while (resultSet.next()) {
              val trade: String = resultSet.getString("trade")
              map += ("BA@" + trade -> 1)
            }
          } catch {
            case e:Exception => e.printStackTrace()
          } finally {
             if(connection !=null){
               connection.close()
             }
          }
    }
    map
  }
}
