package com.hailian.scala.utils

import com.typesafe.config.ConfigFactory
/**
  * 海联软件大数据
  * FileName: GlobalConfigUtils
  * Author: Luyj
  * Date: 2020/5/17 下午10:06
  * Description:
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  * Luyj              2020/5/17 下午10:06    v1.0              创建 
  */
class GlobalConfigUtils {

  private def conf = ConfigFactory.load()
  def  heartColumnFamily = "MM"
  val getProp = (argv:String) => {
    conf.getString(argv)
  }
}

object GlobalConfigUtils extends GlobalConfigUtils
