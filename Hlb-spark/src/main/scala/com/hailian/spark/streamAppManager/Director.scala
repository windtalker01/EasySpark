package com.hailian.spark.streamAppManager

/**
  * 海联软件大数据
  * FileName: Director
  * Author: Luyj
  * Date: 2020/6/2 下午2:28
  * Description:构造器导演类
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  * Luyj              2020/6/2 下午2:28    v1.0              创建 
  */
class Director {
  var tBuilder:Tbuilder = _
  def construct = {
    val streamApp = new StreamApp
    tBuilder.actorFunc(streamApp)
    streamApp
  }
}
