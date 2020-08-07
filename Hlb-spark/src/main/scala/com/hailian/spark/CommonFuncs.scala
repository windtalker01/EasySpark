package com.hailian.spark

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializerFeature
import com.hailian.java.elasticsearch.ESTransportUtils
import com.hailian.scala.utils.Logging
import org.apache.commons.cli._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Copyright (C), 2015-2019, 乐信云科技有限公司
  * FileName: CommonFuncs
  * Author: Luyj
  * Date: 2020/3/5 下午1:30
  * Description:
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  * Luyj              2020/3/5 下午1:30    v1.0              创建 
  */
object CommonFuncs extends Logging{
  def initCmds(args: Array[String], optionMap: mutable.HashMap[String, (Boolean, String)]): CommandLine = {
    var cmd: CommandLine = null
    val options: Options = new Options()

    try {//将定义的option信息填充到Options
      for ((k, v) <- optionMap) {
        val option: Option = new Option(k, v._1, v._2)
        option.setRequired(v._1)
        options.addOption(option)
      }
      val parser = new PosixParser()
      cmd = parser.parse(options, args)//解析命令行参数数组成commandline
    } catch {
      case ex: MissingOptionException  =>
        ex.printStackTrace()
        //如果报错则输出帮助信息
        val formatter: HelpFormatter = new HelpFormatter()
        formatter.printHelp("--help", options)
    }
    cmd
  }
  /** *
    *
    * @Param [optionMap] //参数
    *        @[Java]Param [optionMap]//java参数
    * @description 解析命令行参数 //描述
    * @author luyj //作者
    * @date 2020/3/5 下午1:34 //时间
    * @return _root_.org.apache.commons.cli.CommandLine  //返回值
    *         @[java]return org.apache.commons.cli.CommandLine //java返回值
    */
  def initCmds(args: Array[String], optionMap: mutable.HashMap[String, ((Any, String),Boolean, String)], isLocal: Boolean) = {
    var cmd: CommandLine = null
    val options: Options = new Options()

    try {//将定义的option信息填充到Options
      for ((k, v) <- optionMap) {
        val option: Option = new Option(k, v._2, v._3)
        option.setRequired(v._2)
        options.addOption(option)
      }
      val parser = new PosixParser()
      cmd = parser.parse(options, args)//解析命令行参数数组成commandline
    } catch {
      case ex: MissingOptionException  =>
        ex.printStackTrace()
        //如果报错则输出帮助信息
        val formatter: HelpFormatter = new HelpFormatter()
        formatter.printHelp("--help", options)
    }
    val optionValMap: mutable.HashMap[String, Any] = isLocal match {
      case true => localOption(optionMap)
      case false => serverOption(optionMap, cmd)
    }
    optionValMap
  }

  /**
    * @@Param [spark, dataSQL, optionsMap, saveMode, indexType]
    * @[Java]Param [spark, dataSQL, optionsMap, saveMode, indexType]
    * @@description 根据sql生成dataframe存储到ES
    * @author luyj
    * @@date 2020/4/10 下午3:23
    * @return Unit
    * @[java]return void
    */
  def saveSQL2ES(spark: SparkSession, dataSQL: String, optionsMap: Map[String, String],
                 saveMode: String, indexType: String) ={
    val DF: DataFrame = spark.sql(dataSQL).cache()
    DF.write.format("org.elasticsearch.spark.sql")
      .mode(saveMode)
      .options(optionsMap).save(indexType)
    DF
  }

  /**
    * @@Param [spark, source, optinsMap, file, viewName]
    * @[Java]Param [spark, source, optinsMap, file, viewName]
    * @@description 通过文件生成DATAFRAME
    * @author luyj
    * @@date 2020/4/10 下午4:29
    * @return _root_.org.apache.spark.sql.DataFrame
    * @[java]return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
    */
  def file2DF(spark:SparkSession,source: String, optinsMap: Map[String, String], file: String, viewName: String)={
    val DF: DataFrame = spark.read.format(source).options(optinsMap).load(file)
    DF.createOrReplaceTempView(viewName)
    spark.sqlContext.cacheTable(viewName)
    DF
  }

  def SQL2DF(spark: SparkSession, dataSQL: String, viewName: String) = {
    val DF: DataFrame = spark.sql(dataSQL).cache()
    DF.createOrReplaceTempView(viewName)
    spark.sqlContext.cacheTable(viewName)
    DF
  }


  /**
    * @Param [optionMap]
    * @[Java]Param [optionMap]
    * @description 参数配置的本地提取
    * @author luyj
    * @date 2020/4/10 下午4:30
    * @return _root_.scala.collection.mutable.HashMap[_root_.scala.Predef.String, Any]
    * @[java]return scala.collection.mutable.HashMap<java.lang.String,java.lang.Object>
    */
  def localOption(optionMap: mutable.HashMap[String, ((Any, String),Boolean, String)]) ={
    val map: mutable.HashMap[String, Any] = mutable.HashMap[String, Any]()
    for ((k, v) <- optionMap) {
      map += k -> v._1._1
    }
    map
  }
  /**
    * @Param [optionMap, cmd]
    * @[Java]Param [optionMap, cmd]
    * @description 参数配置的服务器运行提取
    * @author luyj
    * @date 2020/4/10 下午4:31
    * @return _root_.scala.collection.mutable.HashMap[_root_.scala.Predef.String, Any]
    * @[java]return scala.collection.mutable.HashMap<java.lang.String,java.lang.Object>
    */
  def serverOption(optionMap: mutable.HashMap[String, ((Any, String),Boolean, String)], cmd: CommandLine) ={
    val map: mutable.HashMap[String, Any] = mutable.HashMap[String, Any]()
    var tmpVal:Any = null
    for ((k, v) <- optionMap) {
      tmpVal = v._1._1
      if (cmd != null && cmd.hasOption(k)) {
        tmpVal = v._1._2 match {
          case "Text" => cmd.getOptionValue(k)
          case "Int" => cmd.getOptionValue(k).toInt
          case "Double" => cmd.getOptionValue(k).toDouble
          case "Array" => Array(cmd.getOptionValue(k))
          case _ => cmd.getOptionValue(k)
        }
      }
      map += k -> tmpVal
    }
    map
  }
  /**
    * @@Param [rdd, sourceClazz, sparkSession]
    * @@[Java]Param [rdd, sourceClazz, sparkSession]
    * @@description 将对接的json字符串数据RDD,根据定义的bean，转化成dataframe并创建“fromKafka”临时表
    * @@author luyj
    * @@date 2020/6/1 上午8:18
    * @@return _root_.org.apache.spark.sql.DataFrame
    * @@[java]return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
    */
  def jsonStrRDDToDF(rdd:RDD[ConsumerRecord[String, String]], sourceClazz:Class[_], sparkSession: SparkSession) = {
    val begin: Long = System.currentTimeMillis()
    val beanRdd: RDD[Any] = rdd.map(x => {
      //json字符串转换成对象

      var beanInfo = sourceClazz.newInstance()
      try {
        beanInfo = JSON.parseObject(x.value(), sourceClazz)
      } catch {
        case e: Exception => error("/n" + s"Error:JSON解析为ServiceInfoKafka出错./n ${x}")
      }
      beanInfo
    })
    info(s"解析JSON时间：${System.currentTimeMillis() - begin}")
    val begin1: Long = System.currentTimeMillis()
    val frame: DataFrame = sparkSession.createDataFrame(beanRdd, sourceClazz).cache()
    info(s"转换DF时间：${System.currentTimeMillis() - begin1}")
    frame.createOrReplaceTempView("fromKafka")
    frame
  }
  /**
    * @@Param [rdd, sourceClazz, sparkSession, optValMap]
    * @@[Java]Param [rdd, sourceClazz, sparkSession, optValMap]
    * @@description 将对接的json字符串数据RDD,根据定义的bean，转化成dataframe并创建名称与源kafka topic同名的临时表
    * @@author luyj
    * @@date 2020/6/1 上午8:45
    * @@return _root_.org.apache.spark.sql.DataFrame
    * @@[java]return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
    */
  def fromKafkaJsonStrRDDToDF(rdd:RDD[ConsumerRecord[String, String]],
                              sourceClazz:Class[_],
                              sparkSession: SparkSession,
                              optValMap: mutable.Map[String, AnyRef]) = {
    val begin: Long = System.currentTimeMillis()
    val beanRdd: RDD[Any] = rdd.map(x => {
      //json字符串转换成对象

      var beanInfo = sourceClazz.newInstance()
      try {
        val jSONObj: JSONObject = JSON.parseObject(x.value())
        if (jSONObj.containsKey("data")) {
          val dataStr: String = jSONObj.get("data").toString
//          println(s"dataStr: ${dataStr}")
          beanInfo = JSON.parseObject(dataStr, sourceClazz)
        }else{
          beanInfo = JSON.parseObject(x.value(), sourceClazz)
        }

      } catch {
        case e: Exception => error("/n" + s"Error:JSON解析为ServiceInfoKafka出错./n ${x}")
      }
      beanInfo
    })
    info(s"解析JSON时间：${System.currentTimeMillis() - begin}")
    val begin1: Long = System.currentTimeMillis()
    val frame: DataFrame = sparkSession.createDataFrame(beanRdd, sourceClazz).cache()
    info(s"转换DF时间：${System.currentTimeMillis() - begin1}")
    frame.createOrReplaceTempView(optValMap("SourceTopicName").toString)
    frame
  }

  def iterToBeanArr(iter: Iterator[ConsumerRecord[String, String]], sourceClazz:Class[_]) = {
    val begin: Long = System.currentTimeMillis()
    val beanIter = iter.map(x => {
      //json字符串转换成对象

      var beanInfo = sourceClazz.newInstance()
      try {
        beanInfo = JSON.parseObject(x.value(), sourceClazz)
      } catch {
        case e: Exception => println("/n" + s"Error:JSON解析为ServiceInfoKafka出错./n ${x}")
      }
      beanInfo
    })
    println(s"解析JSON时间：${System.currentTimeMillis() - begin}")
    val begin1: Long = System.currentTimeMillis()
    //    val frame: DataFrame = sparkSession.createDataFrame(beanRdd, sourceClazz)
    println(s"转换DF时间：${System.currentTimeMillis() - begin1}")
    //    frame
    beanIter.toArray
  }

  /**
    * @@Param [rdd, esClusterName, esIPs, indexN, typeN, sourceClazz, spark]
    * @@[Java]Param [rdd, esClusterName, esIPs, indexN, typeN, sourceClazz, spark]
    * @@description 根据主键从es对应的索引里查询出记录,并转化成定义的case class组成的df,并使用
    *             case class名称作为注册的表名,并将数据缓存
    * @@author luyj
    * @@date 2020/6/1 上午8:52
    * @@return _root_.org.apache.spark.sql.DataFrame
    * @@[java]return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
    */
  def fromESByIDWithRdd(rdd:RDD[String], esClusterName:String, esIPs:String,indexN:String,typeN:String,sourceClazz:Class[_],spark:SparkSession)={

    val resRdd = rdd.mapPartitions(iter => {
      val esOper = new ESTransportUtils(esClusterName, esIPs)
      var resArr: mutable.Seq[String] = esOper.mget(indexN, typeN, iter.toList).asScala
      resArr = if(resArr.length == 0) mutable.Seq[String](JSON.toJSONString(sourceClazz.newInstance(), SerializerFeature.PrettyFormat)) else resArr
      resArr.toIterator
    }).map(JSON.parseObject(_, sourceClazz))
//    resRdd.foreach(println)
    val frame: DataFrame = spark.createDataFrame(resRdd, sourceClazz)
//      .toDF("sourceid", "closetime", "serviceorderid")
      .cache()
    frame.createOrReplaceTempView(sourceClazz.getSimpleName)
    frame
  }
  def bak ={
    println("test")
  }
}
