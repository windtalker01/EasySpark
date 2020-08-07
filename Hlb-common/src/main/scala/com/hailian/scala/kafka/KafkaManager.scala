package com.hailian.scala.kafka

import java.lang

import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer, NoOffsetForPartitionException}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.zookeeper.data.Stat

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Try

class KafkaManager(zkHost: String, kafkaParams:Map[String, Object]) extends  Serializable {
  private val (zkClient, zkConnection) = ZkUtils.createZkClientAndConnection(zkHost, 10000, 10000)
  private val zkUtils = new ZkUtils(zkClient, zkConnection, false)

  /**
   * @Author Luyj
   * @Description createDirectStream:InputDStream
   * @Date 下午8:02 2020/1/31
   * @Param
   * @return
   **/
  def createDirectStream[k: ClassTag, v:ClassTag](ssc: StreamingContext, topics: Seq[String])={
    //1、读取readOffset
    val groupId = kafkaParams("group.id").toString
    val offsets: Map[TopicPartition, Long] = readOffset(topics, groupId)

    //2、KafkaUtils.createDirectStream -> InputDStream
    val stream: InputDStream[ConsumerRecord[k, v]] = KafkaUtils.createDirectStream[k,v](
      ssc,
      PreferConsistent, //均衡策略，主分区和executor均衡
      ConsumerStrategies.Subscribe[k, v](topics, kafkaParams, offsets)
    )
    stream
  }
  /**
   * @Author Luyj
   * @Description 读取偏移量
   * @Date 下午8:13 2020/1/31
   * @Param
   * @return
   **/
  private def readOffset(topics: Seq[String], groupId: String):Map[TopicPartition, Long] = {
    val topicPartitionMap: mutable.HashMap[TopicPartition, Long] = collection.mutable.HashMap.empty[TopicPartition, Long]
    //(topic -> partitionMap.keys.toSeq.sortWith((s,t) => s < t))
    val topicAndPartitionMaps: mutable.Map[String, Seq[Int]] = zkUtils.getPartitionsForTopics(topics)
    topicAndPartitionMaps.foreach(topicPartitions =>{
      val zkGroupTopicDirs: ZKGroupTopicDirs = new ZKGroupTopicDirs(groupId, topicPartitions._1)
      topicPartitions._2.foreach(partition =>{
        val offsetPath: String = s"${zkGroupTopicDirs.consumerOffsetDir}/${partition}"

        val tryGetTopicPartition: Try[Any] = Try {
          //(dataStr, stat)
          val offsetTuple: (String, Stat) = zkUtils.readData(offsetPath)
          if (offsetTuple != null) {
            //(topic: String, partition: Int)
            topicPartitionMap.put(new TopicPartition(topicPartitions._1, Integer.valueOf(partition)), offsetTuple._1.toLong)
          }
        }
        //如果获取offset失败，则
        if (tryGetTopicPartition.isFailure) {
          val consumer: KafkaConsumer[String, Object] = new KafkaConsumer[String, Object](kafkaParams)
          val topicCllection: List[TopicPartition] = List(new TopicPartition(topicPartitions._1, Integer.valueOf(partition)))
//          val topicPart: TopicPartition = new TopicPartition(topicPartitions._1, Integer.valueOf(partition))
          consumer.assign(topicCllection)
//          val avaliableOffset: lang.Long = consumer.beginningOffsets(topicCllection).values().head
          val avaliableOffset: Long = consumer.position(topicCllection.head)
          consumer.close()
          topicPartitionMap.put(new TopicPartition(topicPartitions._1, Integer.valueOf(partition)), avaliableOffset)

        }
      })
    })
    //TODO 矫正
    val earliestOffsets = getEarliestOffsets(kafkaParams, topics)
    val latestOffsets = getLatestOffsets(kafkaParams, topics)
    for ((k, v) <- topicPartitionMap) {
      val current = v
      val earliest = earliestOffsets.get(k).get
      val lateset = latestOffsets.get(k).get
      if (current < earliest || current > lateset) {
        topicPartitionMap.put(k , earliest)
      }
    }
    topicPartitionMap.toMap
  }
  def getEarliestOffsets(params:Map[String, Object], topics:Seq[String]) ={
    val newKafkaParams = mutable.Map[String, Object]()
    newKafkaParams ++= params
    newKafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    //kafka api
    val consumer: KafkaConsumer[String, Array[Byte]] = new KafkaConsumer[String, Array[Byte]](newKafkaParams)
    //订阅
    consumer.subscribe(topics)
//    val noOffsetForPartitionExceptionSet = mutable.Set()
    try {
      consumer.poll(0)
    } catch {
      case e:NoOffsetForPartitionException =>
//        noOffsetForPartitionExceptionSet.add(e.partition())
    }
    //获取分区信息
    val topicp = consumer.assignment().toSet
    //暂停消费
    consumer.pause(topicp)
    //从头开始
    consumer.seekToBeginning(topicp)
    val earliestOffsetMap: Map[TopicPartition, Long] = topicp.map(line => line -> consumer.position(line)).toMap
    consumer.unsubscribe()
    consumer.close()
    earliestOffsetMap
  }
  def getLatestOffsets(params:Map[String, Object], topics:Seq[String]) ={
    val newKafkaParams = mutable.Map[String, Object]()
    newKafkaParams ++= params
    newKafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    //kafka api
    val consumer: KafkaConsumer[String, Array[Byte]] = new KafkaConsumer[String, Array[Byte]](newKafkaParams)
    //订阅
    consumer.subscribe(topics)
//    val noOffsetForPartitionExceptionSet = mutable.Set()
    try {
      consumer.poll(0)
    } catch {
      case e:NoOffsetForPartitionException =>
//        noOffsetForPartitionExceptionSet.add(e.partition())
    }
    //获取分区信息
    val topicp = consumer.assignment().toSet
    //暂停消费
    consumer.pause(topicp)
    //从头开始
    consumer.seekToEnd(topicp)
    val latestOffsetMap: Map[TopicPartition, Long] = topicp.map(line => line -> consumer.position(line)).toMap
    consumer.unsubscribe()
    consumer.close()
    latestOffsetMap
  }
  def persistOffset[K, V](rdd: RDD[ConsumerRecord[K,V]], storeOffset: Boolean = true) ={
    val groupId: String = kafkaParams("group.id").toString
    val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

    offsetRanges.foreach(or =>{
      //创建组路径
      val zkGroupTopicDirs: ZKGroupTopicDirs = new ZKGroupTopicDirs(groupId, or.topic)
      val offsetPath: String = s"${zkGroupTopicDirs.consumerOffsetDir}/${or.partition}"
      val data = if(storeOffset) or.untilOffset else or.fromOffset
      zkUtils.updatePersistentPath(offsetPath, data+"")

    })
  }
  

}
