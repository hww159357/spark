package cn.itcast.dmp.tools

import java.util.{Calendar, Date}

import org.apache.commons.lang.time.FastDateFormat

//处理时间的工具类
object DateUtils {

  //获取每天的日期  yyyy-MM-dd HH:mm:ss
  def getNowDate():String={
     val date = new Date
    //SimplaDateFormat去操作时间，它是线程不安全，可以使用FastDateFormat 它是线程安全
     val instance: FastDateFormat = FastDateFormat.getInstance("yyyyMMdd HH:mm:ss")
     val now: String = instance.format(date)

    // yyyyMMdd HH:mm:ss ------>  yyyyMMdd
    now.substring(0,8)

  }


  def getYesterDay(): String ={
      val format: FastDateFormat = FastDateFormat.getInstance("yyyyMMdd")
      val calendar: Calendar = Calendar.getInstance()

       calendar.add(Calendar.DATE,-1)

       val time: String = format.format(calendar.getTime)
    time
  }
}
