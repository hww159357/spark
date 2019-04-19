package cn.itcast.dmp.`trait`

//todo:给用户打标签
trait Tags {

  /**
    * 给用户打标签的方法
    * @param args    它表示参数的个数和类型不规定
    * @return Map[String,Double]---->key：标签名称，value:标签权重
    */
  def makeTags(args:Any*):Map[String,Double]
}
