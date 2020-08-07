package com.hailian.spark

import java.util.Properties

import com.hailian.java.utils.StringUtils
import com.hailian.scala.kafka.KafkaZookeeperCheckPoint
import com.hailian.scala.utils.Logging
import com.hailian.scala.utils.kafka.KafkaSink
import com.hailian.spark.common.Constants
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.Try

/***
  *
  *Hailian Software Technology Company
  *
  *This is a module to initialize spark resources. Common parameters will be set
  *directly. For differentiated configurations, a list configuration is provided here.
  *For users with deeper knowledge, you can directly transfer the parameter list for configuration
  *
  * Attention:
  * The following parameters are fixed optimization parameters. Only the parameters in the following list can be
  * configured in the program. Other specific optimization parameters will not work. They need to be configured
  * directly in the running script.
  *
  *
  *All parameters are listed as follows:
  *["isLocalRun",true] Boolean //是否调试运行,调试运行时会setMaster、maxRatePerPartition为2 必须设置
  *["appName", <your appName>]//应用名称，必须设置
  *["esNodes","10.155.20.86"]
  *["esRestPort","9200"]
  *["esClusterName","lseuat"]
  *["registerBeans", Array(classOf[LayoutRelevanceBean])]
  *  ["spark.worker.timeout"]
  *  ["spark.rpc.askTimeout"]
  *  ["spark.task.maxFailures"]
  *  ["spark.driver.allowMutilpleContext" ]
  *  ["spark.serializer"]
  *  ["enableSendEmailOnTaskFail"]
  *  ["spark.buffer.pageSize" ]
  *  ["spark.streaming.backpressure.enabled"]
  *  ["spark.streaming.backpressure.initialRate"]
  *  ["spark.streaming.backpressure.pid.minRate"]
  *  ["spark.speculation"]
  *  ["spark.speculation.interval"]
  *  ["spark.speculation.quantile"]
  **
  optMap:
  [AppName]:spark任务的名称，示例如下：
  AppName: SparkStreamingServiceOrderOnline
  [BatchTime]:spark任务批次时间，示例如下：
  BatchTime: 10
  [kafka服务器列表]：
  KafkaBrokerLst: "10.155.20.89:9092,10.155.20.90:9092,10.155.20.91:9092"
  [KafkaGroupID]:消费组ID
  KafkaGroupID: StreamingServiceOrderTest001001003
  [SourceTopicName]:消费的topic名称
  SourceTopicName: pre_lecc_service_order000001
  [KafkaOffsetReset]:重置消费
  KafkaOffsetReset: earliest
  [ZkList]:存储offset的zk集群的信息
  ZkList: "10.155.20.89:2181,10.155.20.90:2181,10.155.20.91:2181"
  [ZkConsPath]:zk消费者目录，与kafka服务配置的根目录相关，/kafkauat
  ZkConsPath: /kafkauat/consumers
  */
object SparkEngine extends Logging{
  var sparkSession: SparkSession = _
  var sparkContext: SparkContext = _
  var sparkStreaming: StreamingContext = _
  var sqlContext: SQLContext = _
  var kafkaParams: Map[String, Object] = _
  var broadCastKafkaProducer:Broadcast[KafkaSink[String, String]] = _
  var inputStream:InputDStream[ConsumerRecord[String, String]] = _
  class Builder extends Logging{

  }


  def apply(sparkConfMap: Map[String, Any],optMap: mutable.Map[String, AnyRef], logLevel: String) ={
    val sparkConf: SparkConf = getSparkConf(sparkConfMap)
    val batchTime = optMap("BatchTime").toString.toLong //批次时间
    val kafkaList = optMap("KafkaBrokerLst").toString
    val kafkaGroupId = optMap("KafkaGroupID").toString
    val kafkaOffset = optMap("KafkaOffsetReset").toString
    val topicStr = optMap("SourceTopicName").toString
    val zKStr: String = optMap("ZkList").toString
    val zKConsPath: String = optMap("ZkConsPath").toString
    val topics = if ( topicStr != null) topicStr.split(",").toArray else Array("")
    sparkSession = getSparkSession(sparkConf, logLevel)
    sparkContext = sparkSession.sparkContext
    sqlContext = sparkSession.sqlContext
    sparkStreaming = new StreamingContext(sparkContext, Seconds(batchTime))
    setKafkaParams(kafkaList,kafkaGroupId, kafkaOffset)
    broadCastKafkaProducer = getKafkaProducer(kafkaList)
    //初始化kafka连接object单例，手动offset管理
    val zkAccess: Try[Unit] = Try{KafkaZookeeperCheckPoint.init(zKStr, zKConsPath)}
    if (zkAccess.isFailure) {error("offset管理，初始化访问失败！")}
    getInputStream(topics, kafkaGroupId)

  }
  def apply(sparkConfMap: Map[String, Any], logLevel: String, batchTime: Int,
            kafkaList: String, topics: Array[String], kafkaGroupId: String,
            kafkaOffset:String) ={
    val sparkConf: SparkConf = getSparkConf(sparkConfMap)
    sparkSession = getSparkSession(sparkConf, logLevel)
    sqlContext = sparkSession.sqlContext
    sparkContext = sparkSession.sparkContext
    sparkStreaming = new StreamingContext(sparkContext, Seconds(batchTime))
    setKafkaParams(kafkaList,kafkaGroupId, kafkaOffset)
    broadCastKafkaProducer = getKafkaProducer(kafkaList)
    getInputStream(topics, kafkaGroupId)

  }
  def apply(sparkConfMap: Map[String, Any], logLevel: String, batchTime: Int)={
    val sparkConf: SparkConf = getSparkConf(sparkConfMap)
    sparkSession = getSparkSession(sparkConf, logLevel)
    sqlContext = sparkSession.sqlContext
    sparkContext = sparkSession.sparkContext
    sparkStreaming = new StreamingContext(sparkContext, Seconds(batchTime))
  }

  def apply(sparkConfMap: Map[String, Any], logLevel: String)={
    val sparkConf: SparkConf = getSparkConf(sparkConfMap)
    sparkSession = getSparkSession(sparkConf, logLevel)
    sqlContext = sparkSession.sqlContext
    sparkContext = sparkSession.sparkContext
  }
  def getSparkConf(map: Map[String, Any]):SparkConf = {
    if(!map.contains("isLocalRun") || !map.contains("appName"))
      sys.error(
        """
          |These parameters must be set:
          |["isLocalRun",true] Boolean //是否调试运行
          |["appName", <your appName>]//应用名称
        """.stripMargin)
    val confMap: mutable.Map[String, Any] = collection.mutable.Map[String, Any]()
    confMap ++= map
    val sparkConf: SparkConf = new SparkConf().setAppName(confMap.get("appName").get.toString)
    sparkConf.set("spark.worker.timeout" , confMap.getOrElse("spark.worker.timeout" ,
      Constants.SPARK_WORKER_TIMEOUT).toString)
    sparkConf.set("spark.rpc.askTimeout" , confMap.getOrElse("spark.rpc.askTimeout" ,
      Constants.SPARK_RPC_ASKTIMEOUT).toString)
    sparkConf.set("spark.task.maxFailures" , confMap.getOrElse("spark.task.maxFailures" ,
      Constants.SPARK_TASK_MAXFAILURES).toString)
    sparkConf.set("spark.driver.allowMutilpleContext" , confMap.getOrElse("spark.driver.allowMutilpleContext" ,
      Constants.SPARK_DRIVER_ALLOWMUTILPLECONTEXT).toString)
    sparkConf.set("spark.serializer", confMap.getOrElse("spark.serializer" ,
      "org.apache.spark.serializer.KryoSerializer").toString)
    sparkConf.set("enableSendEmailOnTaskFail", confMap.getOrElse("enableSendEmailOnTaskFail" ,
      Constants.ENABLE_SEND_EMAIL_ON_TASK_FAIL).toString)
    sparkConf.set("spark.buffer.pageSize" , confMap.getOrElse("spark.buffer.pageSize" ,
      Constants.SPARK_BUFFER_PAGESIZE).toString)
    sparkConf.set("spark.streaming.backpressure.enabled" , confMap.getOrElse("spark.streaming.backpressure.enabled" ,
      Constants.SPARK_STREAMING_BACKPRESSURE_ENABLED).toString)
    sparkConf.set("spark.streaming.backpressure.initialRate" , confMap.getOrElse("spark.streaming.backpressure.initialRate" ,
      Constants.SPARK_STREAMING_BACKPRESSURE_INITIALRATE).toString)
    sparkConf.set("spark.streaming.backpressure.pid.minRate",confMap.getOrElse("spark.streaming.backpressure.pid.minRate" ,
      Constants.SPARK_STREAMING_BACKPRESSURE_PID_MINRATE).toString)
    sparkConf.set("spark.speculation", confMap.getOrElse("spark.speculation" ,
      Constants.SPARK_SPECULATION_BOOL).toString)
    sparkConf.set("spark.speculation.interval", confMap.getOrElse("spark.speculation.interval" ,
      Constants.SPARK_SPECULATION_INTERVAL).toString)
    sparkConf.set("spark.speculation.quantile",confMap.getOrElse("spark.speculation.quantile" ,
      Constants.SPARK_SPECULATION_QUANTILE).toString)
    if(confMap("isLocalRun").asInstanceOf[Boolean]){//true代表本地程序调试，设置local 及消费速度
      sparkConf.setMaster("local[4]")
        .set("spark.streaming.kafka.maxRatePerPartition" , Constants.SPARK_STREAMING_KAFKA_MAXRATEPERPARTITION_LOCAL)
        .set("spark.sql.shuffle.partitions", Constants.SPARK_SQL_SHUFFLE_PARTITION)
    }
    if(confMap.contains("registerBeans"))
        sparkConf.registerKryoClasses(confMap("registerBeans").asInstanceOf[Array[Class[_]]])
    //处理额外的设置
    val moreSettings: mutable.Map[String, Any] = getMoreSettings(confMap)
    for ((k, v) <- moreSettings) {
      sparkConf.set(k,v.toString)
    }
    sparkConf
  }
  def getMoreSettings(confMap: collection.mutable.Map[String, Any])={
    confMap --= List(
      "isLocalRun",
      "appName",
      "esNodes",
      "esRestPort",
      "esClusterName",
      "registerBeans",
      "spark.worker.timeout",
      "spark.rpc.askTimeout",
      "spark.task.maxFailures",
      "spark.driver.allowMutilpleContext",
      "spark.serializer",
      "enableSendEmailOnTaskFail",
      "spark.buffer.pageSize",
      "spark.streaming.backpressure.enabled",
      "spark.streaming.backpressure.initialRate",
      "spark.streaming.backpressure.pid.minRate",
      "spark.speculation",
      "spark.speculation.interval",
      "spark.speculation.quantile"
    )
    confMap
  }


  def getSparkSession(sparkConf:SparkConf, logLevel:String):SparkSession = {
    val sparkSession: SparkSession = SparkSession.builder()
      .config(sparkConf)
      //调度模式
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.executor.memoryOverhead", "512")//堆外内存
      .config("enableSendEmailOnTaskFail", "true")
//      .config("spark.extraListeners", "com.cartravel.spark.SparkAppListener")//TODO 离线
//      .enableHiveSupport() //开启支持hive
      .getOrCreate()
    sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", "n1")
    sparkSession.sparkContext.setLogLevel(logLevel)
    udfRegister(sparkSession.sqlContext)//注册自定义函数
    sparkSession
  }
  def udfRegister(sqlContext:SQLContext) = {
    sqlContext.udf.register("uuid", StringUtils.uniqualUUID _)
    sqlContext.udf.register("phone_parse", StringUtils.getPhone _)
  }

  def setKafkaParams(kafkaList:String, groupId: String, offsetReset: String) ={
    kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaList,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,// 指定消费方式
      "auto.offset.reset" -> offsetReset,// 是否自动提交
      "security.protocol" -> "SASL_PLAINTEXT",
      "sasl.kerberos.service.name" -> "kafka",
      "sasl.mechanism" -> "GSSAPI",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    kafkaParams
  }

  def getKafkaProducer(kafkaList: String)={
    val kafkaProducerConfig = {
      val p = new Properties
      p.setProperty("bootstrap.servers", kafkaList)
      p.setProperty("key.serializer", classOf[StringSerializer].getName)
      p.setProperty("value.serializer", classOf[StringSerializer].getName)
      p
    }
    sparkContext.broadcast( KafkaSink[String, String](kafkaProducerConfig))
  }


  def getInputStream(topics: Array[String], kafkaGroupId: String)={
    inputStream = KafkaZookeeperCheckPoint.createMyZookeeperDirectKafkaStream(sparkStreaming, kafkaParams, topics, kafkaGroupId)
  }

  def commitOffset(rdd: RDD[ConsumerRecord[String, String]])={
    KafkaZookeeperCheckPoint.storeOffsets(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, kafkaParams("group.id").toString)
  }
}
//def getSparkConf():SparkConf = {
//  val sparkConf: SparkConf = new SparkConf()
//  .set("spark.worker.timeout" , GlobalConfigUtils.getProp("spark.worker.timeout"))
//  //      .set("spark.cores.max" , GlobalConfigUtils.getProp("spark.cores.max"))
//  .set("spark.rpc.askTimeout" , GlobalConfigUtils.getProp("spark.rpc.askTimeout"))
//  .set("spark.task.maxFailures" , GlobalConfigUtils.getProp("spark.task.maxFailures"))
//  .set("spark.driver.allowMutilpleContext" , GlobalConfigUtils.getProp("spark.driver.allowMutilpleContext"))
//  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//  //      .set("spark.sql.adaptive.enabled" , "true")
//  //      .set("spark.streaming.kafka.maxRatePerPartition" , GlobalConfigUtils.getProp("spark.streaming.kafka.maxRatePerPartition"))
//  .set("spark.streaming.backpressure.enabled" , GlobalConfigUtils.getProp("spark.streaming.backpressure.enabled"))
//  .set("spark.streaming.backpressure.initialRate" , GlobalConfigUtils.getProp("spark.streaming.backpressure.initialRate"))
//  .set("spark.streaming.backpressure.pid.minRate","10")
//  .set("enableSendEmailOnTaskFail", "true")
//  .set("spark.buffer.pageSize" , "16m")
//  //      .set("spark.streaming.concurrentJobs" , "5")
//  .set("spark.driver.host", "localhost")
//  .setMaster("local[*]")
//  .setAppName("query")
//  sparkConf.set("spark.speculation", "true")
//  sparkConf.set("spark.speculation.interval", "300")
//  sparkConf.set("spark.speculation.quantile","0.9")
//  sparkConf.set("spark.streaming.backpressure.initialRate" , "500")
//  sparkConf.set("spark.streaming.backpressure.enabled" , "true")
//  sparkConf.set("spark.streaming.kafka.maxRatePerPartition" , "5000")
//  sparkConf.registerKryoClasses(
//  Array(
//  classOf[LayoutRelevanceBean]
//  )
//  )
//  sparkConf
//}
//
//
//  def getSparkSession(sparkConf:SparkConf):SparkSession = {
//  val sparkSession: SparkSession = SparkSession.builder()
//  .config(sparkConf)
//  //调度模式
//  .config("spark.scheduler.mode", "FAIR")
//  .config("spark.executor.memoryOverhead", "512")//堆外内存
//  .config("enableSendEmailOnTaskFail", "true")
//  .config("spark.extraListeners", "com.cartravel.spark.SparkAppListener")//TODO 离线
//  .enableHiveSupport() //开启支持hive
//  .getOrCreate()
//  sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", "n1")
//  sparkSession
//}
