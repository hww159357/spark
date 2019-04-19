package cn.itcast.dmp.es

import cn.itcast.dmp.`trait`.ProcessData
import cn.itcast.dmp.bean.TagsBean
import cn.itcast.dmp.tools.{DateUtils, GlobalConfigUtils}
import org.apache.kudu.spark.kudu._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Data2ES  extends ProcessData{
  private val kuduMaster: String = GlobalConfigUtils.kuduMaster
  private val tableName: String = GlobalConfigUtils.getTagsPrefix+DateUtils.getNowDate()

  val kuduOption=Map(
    "kudu.master" -> kuduMaster,
    "kudu.table"  -> tableName
  )

  override def process(sparkSession: SparkSession, kuduContext: KuduContext): Unit = {
    //1、读取标签表的数据
      val tagsDF: DataFrame = sparkSession.read.options(kuduOption).kudu
      val tagsRDD: RDD[Row] = tagsDF.rdd

    //2、处理数据
    val result: RDD[(String, Map[String, String])] = processTags(tagsRDD)

    //3、数据保存到es中

      import org.elasticsearch.spark._
    //指定数据保存到es中索引库名称和类型
    result.saveToEsWithMeta("dmp/tags")


  }

  def processTags(tagsRDD: RDD[Row])={
    val result: RDD[(String, Map[String, String])] = tagsRDD.map(row => {
      //(IDFA:ETKSHJMYLBRKDFWWEPXCEMDKCZZOOIZD,0)(OPENUDID:YCUGRHBYEXMGWWNVRWVJNRXIFDJHEGQZHDROOTQC,0)
      val userids: String = row.getAs[String]("userids")

      //(SEX@男,3.84)(CZ@济南市,1.92)(PZ@山东省,1.92)
      val tags: String = row.getAs[String]("tags")
      val tags_tmp = tags.substring(1, tags.length - 1) //  SEX@男,3.84)(CZ@济南市,1.92)(PZ@山东省,1.92
      val tagsArray: Array[String] = tags_tmp.split("\\)\\(") //  SEX@男,3.84 CZ@济南市,1.92 PZ@山东省,1.92

      val obj = new TagsBean
      tagsArray.foreach(line => {
        val array: Array[String] = line.split(",") //SEX@男 3.84
        val k: String = array(0)
        val v: String = array(1)

        k match {
          //广告位的类型标签  PC   ("PC@插屏" -> 1 )(PC@全屏,1.92)
          case x if (x.startsWith("PC")) => obj.setPc((x.substring(3, x.length), v))

          //network
          //                    WIFI##D00020001
          //                    4G##D00020002
          //                    3G##D00020003
          //                    2G##D00020004
          //                    NETWORKOTHER##D00020005
          case x if (x.startsWith("D00020001")) => obj.setNetwork(("WIFI", v))
          case x if (x.startsWith("D00020002")) => obj.setNetwork(("4G", v))
          case x if (x.startsWith("D00020003")) => obj.setNetwork(("3G", v))
          case x if (x.startsWith("D00020004")) => obj.setNetwork(("2G", v))
          case x if (x.startsWith("D00020004")) => obj.setNetwork(("其他", v))

          //isp
          //                  移动##D00030001
          //                  联通##D00030002
          //                  电信##D00030003
          //                  OPERATOROTHER##D00030004
          case x if (x.startsWith("D00030001")) => obj.setIsp(("移动", v))
          case x if (x.startsWith("D00030002")) => obj.setIsp(("联通", v))
          case x if (x.startsWith("D00030003")) => obj.setIsp(("电信", v))
          case x if (x.startsWith("D00030004")) => obj.setIsp(("其他", v))

          //app
          //(APP@QQ音乐,1.92)
          case x if (x.startsWith("APP")) => obj.setApp((x.substring(4, x.length), v))

          //pz
          //(PZ@山东省,1.92)
          case x if (x.startsWith("PZ")) => obj.setPz((x.substring(3, x.length), v))

          //cz
          //(CZ@济南市,1.92)
          case x if (x.startsWith("CZ")) => obj.setCz((x.substring(3, x.length), v))

          //os
          //                  1##D00010001
          //                  2##D00010002
          //                  3##D00010003
          //                  4##D00010004

          case x if (x.startsWith("D00010001")) => obj.setOs(("ios", v))
          case x if (x.startsWith("D00010002")) => obj.setOs(("android", v))
          case x if (x.startsWith("D00010003")) => obj.setOs(("wp", v))
          case x if (x.startsWith("D00010004")) => obj.setOs(("其他", v))

          //keywords
          //(K@美文,1.92)
          case x if (x.startsWith("K")) => obj.setKeywords((x.substring(2, x.length), v))

          //sex
          //(SEX@女,1.92)
          case x if (x.startsWith("SEX")) => obj.setSex((x.substring(4, x.length), v))

          //age
          //(AGE@19,1.92)
          case x if (x.startsWith("AGE")) => obj.setAge((x.substring(4, x.length), v))

          //ba
          //(BA@东门:长途汽车站:经一路,1.92)
          case x if (x.startsWith("BA")) => obj.setBa((x.substring(3, x.length), v))
        }


      })

      (userids, obj.toData)
    })
    result

  }
}
