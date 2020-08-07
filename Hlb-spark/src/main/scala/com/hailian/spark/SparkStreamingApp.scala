package com.hailian.spark

import java.text.SimpleDateFormat
import java.util
import com.hailian.scala.utils.kafka.KafkaSink
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * 海联软件大数据
  * FileName: SparkStreamingApp
  * Author: Luyj
  * Date: 2020/5/6 上午7:55
  * Description:
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  * Luyj              2020/5/6 上午7:55    v1.0              创建 
  */
object SparkStreamingApp {
  /**
  * @@Param [optValMap, sparkConfMap, dataFunc]
  * @@[Java]Param [optValMap, sparkConfMap, dataFunc]
  * @@description 实时流程序封装，调用者提供业务处理函数
    * 从单个kafka里读取数据，实时处理。以下参数必须配置
    * AppName: SparkStreamingServiceOrderOnline
    * BatchTime: 10
    * KafkaBrokerLst: "10.155.20.89:9092,10.155.20.90:9092,10.155.20.91:9092"
    * KafkaGroupID: StreamingServiceOrderTest0010010048
    * SourceTopicName: pre_lecc_service_order000001
    * KafkaOffsetReset: earliest
  * @@author luyj
  * @@date 2020/5/6 下午2:33
  * @@return Unit
  * @@[java]return void
  */
  def doStreaming(logLevel:String,
                   optValMap: mutable.Map[String, AnyRef],
                  sparkConfMap: Map[String, Any],
                  dataFunc:(RDD[ConsumerRecord[String, String]], SparkSession) => Unit)={
    SparkEngine(sparkConfMap,optValMap,logLevel)
    System.setProperty("user.name", "hdfs")
    val sparkSession: SparkSession = SparkEngine.sparkSession
    val sqlContext: SQLContext = SparkEngine.sparkSession.sqlContext
    val message: InputDStream[ConsumerRecord[String, String]] = SparkEngine.inputStream
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = SparkEngine.broadCastKafkaProducer

    message.foreachRDD(rdd =>{
      val format: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS")
      println(s"时间：${format.format(System.currentTimeMillis())}, 判断batch rdd是否有数据:${!rdd.isEmpty()}")
      if (!rdd.isEmpty()) {// 若rdd不为空，则执行以下处理
        val rddBegin: Long = System.currentTimeMillis()//记录批次开始时间

        /*=======================================业务处理===================================*/
        dataFunc(rdd, sparkSession) //针对数据集的纯业务逻辑函数
        /*=======================================业务处理===================================*/
        val count = rdd.count()//统计批次记录数
        SparkEngine.commitOffset(rdd)
        println(s"批次消费数据量：${count}。耗时：${(System.currentTimeMillis() - rddBegin)/1000}s")
      }
    })

    SparkEngine.sparkStreaming.start()
    SparkEngine.sparkStreaming.awaitTermination()
  }
  def streamApp(logLevel:String,
                optValMap: mutable.Map[String, AnyRef],
                dataFunc:(RDD[ConsumerRecord[String, String]], SparkSession, mutable.Map[String, AnyRef]) => Unit)={

    val sparkConfMap: Map[String, Any] = Map(
      "isLocalRun" -> optValMap("IsLocalRun"),
      "appName" -> optValMap("AppName")
    )
    SparkEngine(sparkConfMap, optValMap,logLevel)
//    System.setProperty("user.name", "hdfs")
    val sparkSession: SparkSession = SparkEngine.sparkSession
    val sqlContext: SQLContext = SparkEngine.sparkSession.sqlContext
    val message: InputDStream[ConsumerRecord[String, String]] = SparkEngine.inputStream
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = SparkEngine.broadCastKafkaProducer

    message.foreachRDD(rdd =>{
      val format: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS")
      println(s"时间：${format.format(System.currentTimeMillis())}, 判断batch rdd是否有数据:${!rdd.isEmpty()}")
      if (!rdd.isEmpty()) {// 若rdd不为空，则执行以下处理
      val rddBegin: Long = System.currentTimeMillis()//记录批次开始时间

        /*=======================================业务处理===================================*/
        dataFunc(rdd, sparkSession, optValMap) //针对数据集的纯业务逻辑函数
        /*=======================================业务处理===================================*/
        val count = rdd.count()//统计批次记录数
        SparkEngine.commitOffset(rdd)
        println(s"批次消费数据量：${count}。耗时：${(System.currentTimeMillis() - rddBegin)/1000}s")
      }
    })

    SparkEngine.sparkStreaming.start()
    SparkEngine.sparkStreaming.awaitTermination()
    SparkEngine.sparkStreaming.stop()
  }
}
