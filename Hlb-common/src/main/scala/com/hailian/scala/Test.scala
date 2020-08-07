package com.hailian.scala

import com.hailian.scala.utils.GlobalConfigUtils

/**
  * 海联软件大数据
  * FileName: Test
  * Author: Luyj
  * Date: 2020/5/18 下午2:31
  * Description:
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  * Luyj              2020/5/18 下午2:31    v1.0              创建 
  */
object Test {
  def getCommonConfig(arg:String): Unit ={
    println(s"getCommonConfig:${GlobalConfigUtils.getProp(arg)}")
  }
}
