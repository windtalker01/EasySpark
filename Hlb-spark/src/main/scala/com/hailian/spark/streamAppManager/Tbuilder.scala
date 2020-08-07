package com.hailian.spark.streamAppManager

/**
  * 海联软件大数据
  * FileName: Tbuilder
  * Author: Luyj
  * Date: 2020/6/2 下午2:26
  * Description: 构造器接口
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  * Luyj              2020/6/2 下午2:26    v1.0              创建 
  */
trait Tbuilder {
  def actorFunc(streamApp: StreamApp)
}
