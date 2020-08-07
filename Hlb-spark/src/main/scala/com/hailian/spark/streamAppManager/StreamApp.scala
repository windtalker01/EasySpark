package com.hailian.spark.streamAppManager

import java.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * 海联软件大数据
  * FileName: StreamApp
  * Author: Luyj
  * Date: 2020/6/2 下午1:57
  * Description: app类
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  * Luyj              2020/6/2 下午1:57    v1.0              创建 
  */
class StreamApp {
  var appFunc:(RDD[ConsumerRecord[String, String]], SparkSession, mutable.Map[String, AnyRef]) => Unit = _
}
