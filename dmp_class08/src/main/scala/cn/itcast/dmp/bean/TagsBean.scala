package cn.itcast.dmp.bean

import scala.collection.mutable.ListBuffer

class TagsBean {
  private var os:ListBuffer[(String,String)]=new ListBuffer[(String,String)]()
  private var network:ListBuffer[(String,String)]=new ListBuffer[(String,String)]()
  private var isp:ListBuffer[(String,String)]=new ListBuffer[(String,String)]()
  private var app:ListBuffer[(String,String)]=new ListBuffer[(String,String)]()
  private var pz:ListBuffer[(String,String)]=new ListBuffer[(String,String)]()
  private var cz:ListBuffer[(String,String)]=new ListBuffer[(String,String)]()
  private var pc :ListBuffer[(String,String)]=new ListBuffer[(String,String)]()
  private var keywords :ListBuffer[(String,String)]=new ListBuffer[(String,String)]()
  private var sex :ListBuffer[(String,String)]=new ListBuffer[(String,String)]()
  private var age :ListBuffer[(String,String)]=new ListBuffer[(String,String)]()
  private var ba :ListBuffer[(String,String)]=new ListBuffer[(String,String)]()


   def toData ={
     Map(
       "os" ->os.mkString,
       "network" ->network.mkString,
       "isp" ->isp.mkString,
       "app" ->app.mkString,
       "pz" ->pz.mkString,
       "cz" ->cz.mkString,
       "pc" ->pc.mkString,
       "keywords" ->keywords.mkString,
       "sex" ->sex.mkString,
       "age" ->age.mkString,
       "ba" ->ba.mkString
     )
  }

  def setOs(os:(String,String))={
    this.os.append(os)
  }

  def setNetwork(network:(String,String))={
    this.network.append(network)
  }

  def setIsp(isp:(String,String))={
    this.isp.append(isp)
  }
  def setApp(app:(String,String))={
    this.app.append(app)
  }

  def setPz(pz:(String,String))={
    this.pz.append(pz)
  }

  def setCz(cz:(String,String))={
    this.cz.append(cz)
  }

  def setPc(pc:(String,String))={
    this.pc.append(pc)
  }

  def setKeywords(keywords:(String,String))={
    this.keywords.append(keywords)
  }

  def setBa(ba:(String,String))={
    this.ba.append(ba)
  }

  def setSex(sex:(String,String))={
    this.sex.append(sex)
  }

  def setAge(age:(String,String))={
    this.age.append(age)
  }
}
