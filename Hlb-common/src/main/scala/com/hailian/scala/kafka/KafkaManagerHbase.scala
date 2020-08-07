package com.hailian.scala.kafka

import com.hailian.scala.hbase.hutils.HbaseUtil
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer, NoOffsetForPartitionException}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Try

class KafkaManagerHbase(kafkaParams:Map[String, Object]) extends Serializable {
  //rowkey:topicName   MM:ParNums
  private val hbaseTopics:TableName = TableName.valueOf("topics")
  //rowkey:topicName#groupId#partition  MM:v
  private val hbaseOffsets:TableName = TableName.valueOf("consumerOffset")

  def createDirectStream[K:ClassTag, V:ClassTag](ssc:StreamingContext, topics:Seq[String])={
    val groupId: String = kafkaParams("group.id").toString
    val beginTime = System.currentTimeMillis()
    val tuple  = readOffset(groupId, topics)
    println("readOffset：" + (System.currentTimeMillis() - beginTime) / 1000 + "s")
    var kafkaInputDStream:InputDStream[ConsumerRecord[K, V]] = null
    if(tuple._2 == 0){//如果没有没从头消费
      kafkaInputDStream = KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(topics, kafkaParams)
      )
      println("首次 Streaming 成功！")
    }else if (tuple._2 == 1){
      kafkaInputDStream = KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(topics,kafkaParams,tuple._1)
      )
      println(tuple._1)
      println("中断后 Streaming 成功！")
    }

    kafkaInputDStream
  }
  //1、读取对应的topicName+groupId+partition的offset,
  // rowkey:000#topicName#groupID,partition序号左补0，长度为3位，尽量保障长度为16位
  //1.1 partition都是从0开始的，先判断rowkey:000#topicName#groupID是否有值
  private def readOffset(groupId: String, topics:Seq[String]):(Map[TopicPartition, Long], Int) ={
    //初始化topic/partition的offset
    val topicPartitionAndOffset: mutable.HashMap[TopicPartition, Long] = collection.mutable.HashMap.empty[TopicPartition, Long]
    //获取HBASE连接
    val hbaseConn: Connection = HbaseUtil.connection
    val beginTime = System.currentTimeMillis()
    val topicPartitions: (Map[String, Seq[Int]], Int) = getTopicAndPars(topics, hbaseConn)
    println("getTopicAndPars：" + (System.currentTimeMillis() - beginTime) / 1000 + "s")

    val beginTime1 = System.currentTimeMillis()
    if(topicPartitions._2 == 1) {
      topicPartitions._1.foreach(topicPars => {

        //构建offset记录的hbase table
        val offsetTab: Table = hbaseConn.getTable(hbaseOffsets)
        topicPars._2.foreach(partition => {
          val tryGetTopicPartition = Try {
            //根据rowkey:topicName#groupId#partition,构建get
            val g: Get = new Get(s"${topicPars._1}#${groupId}#${partition}".getBytes())
            val result: Result = offsetTab.get(g)
            //获取offset值

            val offset: Long = Bytes.toLong(result.getValue("MM".getBytes, "v".getBytes))
            topicPartitionAndOffset.put(new TopicPartition(topicPars._1, Integer.valueOf(partition)), offset)

          }
          if (tryGetTopicPartition.isFailure) {//如果从hbase获取失败，从头消费
            val consumer: KafkaConsumer[String, Object] = new KafkaConsumer[String, Object](kafkaParams)
            val topicCllection: List[TopicPartition] = List(new TopicPartition(topicPars._1, Integer.valueOf(partition)))
            //          val topicPart: TopicPartition = new TopicPartition(topicPartitions._1, Integer.valueOf(partition))
            consumer.assign(topicCllection)
            //          val avaliableOffset: lang.Long = consumer.beginningOffsets(topicCllection).values().head
            val avaliableOffset: Long = consumer.position(topicCllection.head)
            consumer.close()
            topicPartitionAndOffset.put(new TopicPartition(topicPars._1, Integer.valueOf(partition)), avaliableOffset)

          }
        })


        offsetTab.close()
      })
    }
      println("end：" + (System.currentTimeMillis() - beginTime1) / 1000 + "s")

    if (!topicPartitionAndOffset.isEmpty) {
      //TODO 纠正offset
      val beginTime2 = System.currentTimeMillis()
      val earliestOffsetMap: Map[TopicPartition, Long] = getEarliestOffsets(kafkaParams,topics)
//      val earliestOffsetMap: Map[TopicPartition, Long] = getEarliestOffsetsV2(kafkaParams,topics)
      println("earliestOffsetMap：" + (System.currentTimeMillis() - beginTime2) / 1000 + "s")
      val beginTime3 = System.currentTimeMillis()
      val latestOffsetMap: Map[TopicPartition, Long] = getLatestOffsets(kafkaParams, topics)
      println("latestOffsetMap：" + (System.currentTimeMillis() - beginTime3) / 1000 + "s")
      for ((k, v) <- topicPartitionAndOffset) {
        val earliestVal: Long = earliestOffsetMap.get(k).get
        val latestVal: Long = latestOffsetMap.get(k).get
        if(v < earliestVal || v > latestVal)
          topicPartitionAndOffset.put(k, earliestVal)
      }

      (topicPartitionAndOffset.toMap,1)
    }else{
      (topicPartitionAndOffset.toMap, 0)
    }
  }
  def persistOffsets[K, V](rdd:RDD[ConsumerRecord[K, V]], persist:Boolean)={
    val groupId: String = kafkaParams("group.id").toString
    val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //生成Map(topicName -> partitionNum)
    val topicParNum: mutable.Map[String, Int] = offsetRanges
      .map(rec => (rec.topic, rec.partition))
      .foldLeft(new mutable.HashMap[String, Int]())((map, topicPa) => {
        if (map.isEmpty ||
          !map.contains(topicPa._1) ||
          map.contains(topicPa._1) && map(topicPa._1) < topicPa._2) {
          map += topicPa
          map.put(topicPa._1, topicPa._2)
        }
        map
      }
      )
    val beginTime = System.currentTimeMillis()
    val hbaseConn: Connection = HbaseUtil.connection

    val topicMulPut = mutable.ListBuffer[Put]()
    //更新主题表
    val topicTab: Table = hbaseConn.getTable(hbaseTopics)
    topicParNum.foreach(rec =>{
      val put = new Put(rec._1.getBytes())
      put.addColumn("MM".getBytes(), "ParNums".getBytes(), Bytes.toBytes(rec._2))
      topicMulPut += put
    })
    topicTab.put(topicMulPut)
    topicTab.close()
    println("hbaseTopics：" + (System.currentTimeMillis() - beginTime) / 1000 + "s")

    //更新offset表
    val offsetMulPut = mutable.ListBuffer[Put]()
    val offsetTab: Table = hbaseConn.getTable(hbaseOffsets)
    offsetRanges.foreach(rec =>{
      val put: Put = new Put(s"${rec.topic}#${groupId}#${rec.partition}".getBytes())
      val data = if(persist) rec.untilOffset else rec.fromOffset
      put.addColumn("MM".getBytes(), "v".getBytes(), Bytes.toBytes(data))
      offsetMulPut += put
      var Nums = 0L
      if(rec.fromOffset != null && rec.untilOffset !=null) Nums = rec.untilOffset - rec.fromOffset else Nums=0L
      println("---Offset写入Hbase---读取量："+Nums+"\nTopic：" + rec.topic + ", Partition:" + rec.partition + ", Offset:" + rec.untilOffset)

    })
    offsetTab.put(offsetMulPut)
    offsetTab.close()
    println("hbaseOffsets：" + (System.currentTimeMillis() - beginTime) / 1000 + "s")

  }
  /*
   * @Author Luyj
   * @Description 获取topic及分区集合
   * @Date 下午3:48 2020/2/26
   * @Param [topics, hbaseConn]
   * @return (_root_.scala.collection.immutable.Map[_root_.scala.Predef.String, scala.Seq[Int]], Int)
   **/
  private def getTopicAndPars(topics: Seq[String], hbaseConn: Connection) ={
    //初始化topic及partition的map
    val topicAndPartitions: mutable.HashMap[String, Seq[Int]] = collection.mutable.HashMap.empty[String, Seq[Int]]
    //构建hbase table
    val topicTab: Table = hbaseConn.getTable(hbaseTopics)
    topics.foreach(topic =>{
      //根据rowkey:topicName构建Get
      val g = new Get(Bytes.toBytes(topic))
      //根据rowkey查询数据
      val result: Result = topicTab.get(g)
      //获取topic的分区信息
      if (result.size() > 0) {
        val parNums: Int = Bytes.toInt(result.getValue("MM".getBytes(), "ParNums".getBytes()))
        topicAndPartitions.put(topic, 0 to parNums)
      }
    })
    topicTab.close()
    //如果传入的topic数量与查询后的topic数量不一致，则传递信息0，否则传递信息1
    if(topicAndPartitions.size != topics.size) (topicAndPartitions.toMap, 0) else (topicAndPartitions.toMap, 1)
  }


  def getEarliestOffsetsV2(params:Map[String, Object], topics:Seq[String]) ={
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
  /*
   * @Author Luyj
   * @Description 根据topic列表获取beginning的offset
   * @Date 下午5:06 2020/2/26
   * @Param [kafkaParams, topics]
   * @return _root_.scala.collection.immutable.Map[_root_.org.apache.kafka.common.TopicPartition, Long]
   **/
  private def getEarliestOffsets(kafkaParams:Map[String, Object], topics:Seq[String]) ={
    val beginTime = System.currentTimeMillis()
    //根据传入的连接kafka的配置，修改auto_offset_reset为earliest
    val newKafkaParams: mutable.Map[String, Object] = mutable.Map[String, Object]()
    println("newKafkaParams：" + (System.currentTimeMillis() - beginTime) / 1000 + "s")
    newKafkaParams ++= kafkaParams
    println("newKafkaParams ++= kafkaParams：" + (System.currentTimeMillis() - beginTime) / 1000 + "s")
    newKafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest")

    var topicPartition :Map[TopicPartition, Long] = null

    //定义消费者
    val consumer = new KafkaConsumer[String, Array[Byte]](newKafkaParams)
    consumer.subscribe(topics)//消费者向topic订阅
    try{
      consumer.poll(0)//消费者拉取
      println("poll：" + (System.currentTimeMillis() - beginTime) / 1000 + "s")
      lazy val topicp: Set[TopicPartition] = consumer.assignment().toSet//获取TopicPartition集合
      println("assignment：" + (System.currentTimeMillis() - beginTime) / 1000 + "s")
      consumer.pause(topicp)//暂停消费
      println("pause：" + (System.currentTimeMillis() - beginTime) / 1000 + "s")
      consumer.seekToBeginning(topicp)//从beginning消费
      println("seekToBeginning：" + (System.currentTimeMillis() - beginTime) / 1000 + "s")
      val t = topicp.map(line => {
        val beginTime1 = System.currentTimeMillis()
        println(beginTime1)
        val pos = consumer.position(line)
        println(s"consumer.position：${line}" + (System.currentTimeMillis() - beginTime1) / 1000 + "s")
        line -> pos}).toMap
      println("topicp.map：" + (System.currentTimeMillis() - beginTime) / 1000 + "s")
      topicPartition = t
      println("topicPartition = t：" + (System.currentTimeMillis() - beginTime) / 1000 + "s")
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      consumer.unsubscribe()
      println("consumer.unsubscribe：" + (System.currentTimeMillis() - beginTime) / 1000 + "s")
      consumer.close()
      println("consumer.close：" + (System.currentTimeMillis() - beginTime) / 1000 + "s")

    }
    topicPartition.toMap

  }

  /*
   * @Author Luyj
   * @Description 根据topic列表获取beginning的offset
   * @Date 下午5:15 2020/2/26
   * @Param [kafkaParams, topics]
   * @return _root_.scala.collection.immutable.Map[_root_.org.apache.kafka.common.TopicPartition, Long]
   **/
  private def getLatestOffsets(kafkaParams:Map[String, Object], topics:Seq[String]) ={
    //根据传入的连接kafka的配置，修改auto_offset_reset为earliest
    val newKafkaParams: mutable.Map[String, Object] = mutable.Map[String, Object]()
    newKafkaParams ++= kafkaParams
    newKafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest")

    val topicPartition: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()

    //定义消费者
    val consumer = new KafkaConsumer[String, Array[Byte]](newKafkaParams)
    consumer.subscribe(topics)//消费者向topic订阅
    try{
      consumer.poll(0)//消费者拉取
      val topicp: Set[TopicPartition] = consumer.assignment().toSet//获取TopicPartition集合
      consumer.pause(topicp)//暂停消费
      consumer.seekToEnd(topicp)//从end消费
      topicPartition ++= topicp.map(line => line -> consumer.position(line)).toMap
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      consumer.unsubscribe()
      consumer.close()
    }
    topicPartition.toMap
  }
}
