package cn.itcast.dmp.tools


//后期整个工程的所有sql语句统一在这里编写
object ContantsSQL {
  //1：初始化，将经纬度和地域省市merge合并到ods中
  lazy val odssql = "select " +
    "ods.ip ," +
    "ods.sessionid," +
    "ods.advertisersid," +
    "ods.adorderid," +
    "ods.adcreativeid," +
    "ods.adplatformproviderid" +
    ",ods.sdkversion" +
    ",ods.adplatformkey" +
    ",ods.putinmodeltype" +
    ",ods.requestmode" +
    ",ods.adprice" +
    ",ods.adppprice" +
    ",ods.requestdate" +
    ",ods.appid" +
    ",ods.appname" +
    ",ods.uuid" +
    ",ods.device" +
    ",ods.client" +
    ",ods.osversion" +
    ",ods.density" +
    ",ods.pw" +
    ",ods.ph" +
    ",ipRegion.longitude as long" +
    ",ipRegion.latitude as lat" +
    ",ipRegion.province as provincename" +
    ",ipRegion.city as cityname" +
    ",ods.ispid, ods.ispname" +
    ",ods.networkmannerid, ods.networkmannername, ods.iseffective, ods.isbilling" +
    ",ods.adspacetype, ods.adspacetypename, ods.devicetype, ods.processnode, ods.apptype" +
    ",ods.district, ods.paymode, ods.isbid, ods.bidprice, ods.winprice, ods.iswin, ods.cur" +
    ",ods.rate, ods.cnywinprice, ods.imei, ods.mac, ods.idfa, ods.openudid,ods.androidid" +
    ",ods.rtbprovince,ods.rtbcity,ods.rtbdistrict,ods.rtbstreet,ods.storeurl,ods.realip" +
    ",ods.isqualityapp,ods.bidfloor,ods.aw,ods.ah,ods.imeimd5,ods.macmd5,ods.idfamd5" +
    ",ods.openudidmd5,ods.androididmd5,ods.imeisha1,ods.macsha1,ods.idfasha1,ods.openudidsha1" +
    ",ods.androididsha1,ods.uuidunknow,ods.userid,ods.iptype,ods.initbidprice,ods.adpayment" +
    ",ods.agentrate,ods.lomarkrate,ods.adxrate,ods.title,ods.keywords,ods.tagid,ods.callbackdate" +
    ",ods.channelid,ods.mediatype,ods.email,ods.tel,ods.sex,ods.age " +
    "from ods left join ipRegion on ods.ip=ipRegion.ip where ods.ip is not null"


  //2、编写地域数量分布情况的sql语句
   lazy  val regionSql="select provincename,cityname,count(*) as num from ods group by provincename,cityname"


  //3、编写广告地域数量分布情况的sql语句
  lazy  val adLocationAnalysisTmpSQL="select  " +
    "provincename,cityname," +
    "sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) as originalRequest," +
    "sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) as validRequest," +
    "sum(case when requestmode=1 and processnode=3  then 1 else 0 end) as adRequest," +
    "sum(case when adplatformproviderid >=100000 and  iseffective=1 and isbilling=1 and isbid=1 and adorderid !=0 then 1 else 0 end ) as bidsNum," +
    "sum(case when adplatformproviderid >=100000 and  iseffective=1 and isbilling=1 and iswin=1 then 1 else 0 end ) as bidsus," +
    "sum(case when requestmode=2 and iseffective=1  then 1 else 0 end) as adImpressions," +
    "sum(case when requestmode=3 and iseffective=1  then 1 else 0 end) as adClicks," +
    "sum(case when requestmode=2 and iseffective=1 and isbilling=1 then 1 else 0 end) as mediumDisplayNum," +
    "sum(case when requestmode=3 and iseffective=1 and isbilling=1 then 1 else 0 end) as mediumClickNum," +
    "sum(case when adplatformproviderid >=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid >200000 and adcreativeid >200000  then 1*winprice/1000 else 0 end) as adCost," +
    "sum(case when adplatformproviderid >=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid >200000 and adcreativeid >200000  then 1*adpayment/1000 else 0 end) as adConsumption " +
    "from  ods   group by provincename,cityname"

  lazy  val adLocationAnylysis="select " +
    "provincename,cityname," +
    "originalRequest,validRequest," +
    "adRequest," +
    "bidsNum," +
    "bidsus," +
    "bidsus/bidsNum as bidsusRat," +
    "adImpressions," +
    "adClicks," +
    "adClicks/adImpressions as adClicksRat," +
    "adCost,adConsumption from adLocationAnalysis where bidsNum !=0 and  adImpressions!=0"


  //4、统计广告投放app的分布情况临时表
  lazy val appAnalysis_temp= "select " +
    "appid , " +
    "appname," +
    "sum(case when requestmode=1 and processnode >=1 then 1 else 0 end) originalRequest, " +
    "sum(case when requestmode=1 and processnode >=2 then 1 else 0 end) validRequest,  " +
    "sum(case when requestmode=1 and processnode =3 then 1 else 0 end) adRequest,  " +
    "sum(case when iseffective=1 and isbilling=1 and isbid=1 and adorderid !=0 and adplatformproviderid >=100000 then 1 else 0 end) bidsNum," +
    "sum(case when iseffective=1 and isbilling=1 and iswin=1 and adplatformproviderid >=100000 then 1 else 0 end) bidsSus," +
    "sum(case when requestmode=2 and iseffective=1 and isbilling=1 then 1 else 0 end) mediumDisplayNum, " +
    "sum(case when requestmode=3 and iseffective=1 and isbilling=1 then 1 else 0 end) mediumClickNum " +
    "from ods group by appid, appname"

  //统计广告投放app的分布情况事实表
  lazy val appAnalysis= "select " +
    "appid," +
    "appname," +
    "originalRequest," +
    "validRequest," +
    "adRequest," +
    "bidsNum," +
    "bidsSus," +
    "bidsSus/bidsNum bidsSusRat," +
    "mediumDisplayNum," +
    "mediumClickNum, " +
    "mediumClickNum/mediumDisplayNum mediumClickRat" +
    " from adAppAnalysis where bidsNum !=0 and mediumDisplayNum !=0"

  //5、统计广告投放手机设备分布情况临时表
  lazy val deviceAnalysis_temp= "select case client " +
    "when 1 then 'ios' " +
    "when 2 then 'android' " +
    "when 3 then 'wp' " +
    "else 'other' end as client," +
    "device," +
    "sum(case when requestmode <=2 and processnode =1  then 1 else 0 end) originalRequest," +
    "sum(case when requestmode >=1 and processnode >=2 then 1 else 0 end) validRequest," +
    "sum(case when requestmode =1  and processnode =3  then 1 else 0 end) adRequest," +
    "sum(case when adplatformproviderid >= 100000 and iseffective =1 and isbilling =1 and isbid =1 and adorderid !=0 then 1 else 0 end)  bidsNum," +
    "sum(case when iseffective=1 and isbilling=1 and iswin=1 and adplatformproviderid >=100000 then 1 else 0 end) bidsSus," +
    "sum(case when requestmode =2 and iseffective =1 then 1 else 0 end )  adImpressions, " +
    "sum(case when requestmode =3 and iseffective =1 then 1 else 0 end )  adClicks," +
    "sum(case when requestmode =2 and iseffective =1 and isbilling =1 then 1 else 0 end )  mediumDisPlayNum," +
    "sum(case when requestmode =3 and iseffective =1 and isbilling =1 then 1 else 0 end )  mediumClickNum," +
    "sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling=1 and iswin =1 and adorderid >200000 and adcreativeid > 200000  then 1*winprice/1000 else 0 end ) adCost," +
    "sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling=1 and iswin =1 and adorderid >200000 and adcreativeid > 200000  then 1*adpayment/1000 else 0 end) adConsumption   " +
    "from ods group by client,device"

  //统计广告投放手机设备分布情况事实表
  lazy val deviceAnalysis= "select " +
    "client," +
    "device," +
    "originalRequest," +
    "validRequest," +
    "adRequest," +
    "bidsNum," +
    "bidsSus," +
    "bidsSus/bidsNum bidsSusRat," +
    "mediumDisplayNum," +
    "mediumClickNum, " +
    "mediumClickNum/mediumDisplayNum mediumClickRat" +
    " from adDeviceAnalysis where bidsNum !=0 and mediumDisplayNum !=0"


  //6、统计广告投放网络类型分布情况临时表
  lazy val networkAnalysis_temp= "select networkmannerid, " +
    "networkmannername, " +
    "sum(case when requestmode <=2 and processnode =1  then 1 else 0 end) originalRequest," +
    "sum(case when requestmode >=1 and processnode >=2 then 1 else 0 end) validRequest," +
    "sum(case when requestmode =1  and processnode =3  then 1 else 0 end) adRequest," +
    "sum(case when adplatformproviderid >= 100000 and iseffective =1 and isbilling =1 and isbid =1 and adorderid !=0 then 1 else 0 end)  bidsNum," +
    "sum(case when iseffective=1 and isbilling=1 and iswin=1 and adplatformproviderid >=100000 then 1 else 0 end) bidsSus," +
    "sum(case when requestmode =2 and iseffective =1 then 1 else 0 end )  adImpressions, " +
    "sum(case when requestmode =3 and iseffective =1 then 1 else 0 end )  adClicks," +
    "sum(case when requestmode =2 and iseffective =1 and isbilling =1 then 1 else 0 end )  mediumDisPlayNum," +
    "sum(case when requestmode =3 and iseffective =1 and isbilling =1 then 1 else 0 end )  mediumClickNum," +
    "sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling=1 and iswin =1 and adorderid >200000 and adcreativeid > 200000  then 1*winprice/1000 else 0 end ) adCost," +
    "sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling=1 and iswin =1 and adorderid >200000 and adcreativeid > 200000  then 1*adpayment/1000 else 0 end) adConsumption   " +
    "from ods group by networkmannerid,networkmannername"

  //统计广告投放网络类型分布情况事实表
  lazy val networkAnalysis= "select " +
    "networkmannerid," +
    "networkmannername," +
    "originalRequest," +
    "validRequest," +
    "adRequest," +
    "bidsNum," +
    "bidsSus," +
    "bidsSus/bidsNum bidsSusRat," +
    "mediumDisplayNum," +
    "mediumClickNum, " +
    "mediumClickNum/mediumDisplayNum mediumClickRat" +
    " from networkAnalysis where bidsNum !=0 and mediumDisplayNum !=0"


  //7、统计广告投放网络运营商分布情况临时表
  lazy val ispAnalysis_temp= "select  " +
    "ispname," +
    "sum(case when requestmode <=2 and processnode =1  then 1 else 0 end) originalRequest," +
    "sum(case when requestmode >=1 and processnode >=2 then 1 else 0 end) validRequest," +
    "sum(case when requestmode =1  and processnode =3  then 1 else 0 end) adRequest," +
    "sum(case when adplatformproviderid >= 100000 and iseffective =1 and isbilling =1 and isbid =1 and adorderid !=0 then 1 else 0 end)  bidsNum," +
    "sum(case when iseffective=1 and isbilling=1 and iswin=1 and adplatformproviderid >=100000 then 1 else 0 end) bidsSus," +
    "sum(case when requestmode =2 and iseffective =1 then 1 else 0 end )  adImpressions, " +
    "sum(case when requestmode =3 and iseffective =1 then 1 else 0 end )  adClicks," +
    "sum(case when requestmode =2 and iseffective =1 and isbilling =1 then 1 else 0 end )  mediumDisPlayNum," +
    "sum(case when requestmode =3 and iseffective =1 and isbilling =1 then 1 else 0 end )  mediumClickNum," +
    "sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling=1 and iswin =1 and adorderid >200000 and adcreativeid > 200000  then 1*winprice/1000 else 0 end ) adCost," +
    "sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling=1 and iswin =1 and adorderid >200000 and adcreativeid > 200000  then 1*adpayment/1000 else 0 end) adConsumption   " +
    "from ods group by ispname"

  //统计广告投放网络运营商情况事实表
  lazy val ispAnalysis= "select " +
    "ispname," +
    "originalRequest," +
    "validRequest," +
    "adRequest," +
    "bidsNum," +
    "bidsSus," +
    "bidsSus/bidsNum bidsSusRat," +
    "mediumDisplayNum," +
    "mediumClickNum, " +
    "mediumClickNum/mediumDisplayNum mediumClickRat" +
    " from ispAnalysis where bidsNum !=0 and mediumDisplayNum !=0"

  //8、统计广告投放渠道分布情况临时表
  lazy val channelAnalysis_temp= "select  " +
    "channelid," +
    "sum(case when requestmode <=2 and processnode =1  then 1 else 0 end) originalRequest," +
    "sum(case when requestmode >=1 and processnode >=2 then 1 else 0 end) validRequest," +
    "sum(case when requestmode =1  and processnode =3  then 1 else 0 end) adRequest," +
    "sum(case when adplatformproviderid >= 100000 and iseffective =1 and isbilling =1 and isbid =1 and adorderid !=0 then 1 else 0 end)  bidsNum," +
    "sum(case when iseffective=1 and isbilling=1 and iswin=1 and adplatformproviderid >=100000 then 1 else 0 end) bidsSus," +
    "sum(case when requestmode =2 and iseffective =1 then 1 else 0 end )  adImpressions, " +
    "sum(case when requestmode =3 and iseffective =1 then 1 else 0 end )  adClicks," +
    "sum(case when requestmode =2 and iseffective =1 and isbilling =1 then 1 else 0 end )  mediumDisPlayNum," +
    "sum(case when requestmode =3 and iseffective =1 and isbilling =1 then 1 else 0 end )  mediumClickNum," +
    "sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling=1 and iswin =1 and adorderid >200000 and adcreativeid > 200000  then 1*winprice/1000 else 0 end ) adCost," +
    "sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling=1 and iswin =1 and adorderid >200000 and adcreativeid > 200000  then 1*adpayment/1000 else 0 end) adConsumption   " +
    "from ods group by channelid"

  //统计广告投放网络运营商情况事实表
  lazy val channelAnalysis= "select " +
    "channelid," +
    "originalRequest," +
    "validRequest," +
    "adRequest," +
    "bidsNum," +
    "bidsSus," +
    "bidsSus/bidsNum bidsSusRat," +
    "mediumDisplayNum," +
    "mediumClickNum, " +
    "mediumClickNum/mediumDisplayNum mediumClickRat" +
    " from channelAnalysis where bidsNum !=0 and mediumDisplayNum !=0"

  //9、过滤出国内的经纬度sql
  lazy  val filter_non_china="select * from ods where long >73 and long < 136 and lat >3 and lat <54"


 //10、过滤出有效的数据
  lazy val non_empty_sql=
    """
      | select * from ods where
      | imei !='' or imeimd5 !='' or imeisha1 !='' or
      | mac  !='' or macmd5 !='' or macsha1 !='' or
      | idfa  !='' or idfamd5 !='' or idfasha1 !='' or
      | openudid  !='' or openudidmd5 !='' or openudidsha1 !='' or
      | androidid  !='' or androididmd5 !='' or androididsha1 !=''
    """.stripMargin

}
