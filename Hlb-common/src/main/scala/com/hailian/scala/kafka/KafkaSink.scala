package com.hailian.scala.utils.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
/**
  * Copyright (C), 2015-2019, 乐信云科技有限公司
  * FileName: KafkaSink
  * Author: Luyj
  * Date: 2020/3/16 上午9:54
  * Description:
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  * Luyj              2020/3/16 上午9:54    v1.0              创建 
  */
class KafkaSink[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {
  /* This is the key idea that allows us to work around running into
     NotSerializableExceptions. */
  lazy val producer: KafkaProducer[K, V] = createProducer()
  /**
  * @@Param [topic, key, value]
  * @[Java]Param [topic, key, value]
  * @@description 根据topic,key,value推送消息
  * @@author luyj
  * @@date 2020/3/16 上午10:05
  * @@return _root_.java.util.concurrent.Future[_root_.org.apache.kafka.clients.producer.RecordMetadata]
  * @[java]return java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata>
  */
  def send(topic: String, key: K, value: V) =
    producer.send(new ProducerRecord[K, V](topic, key, value))
  /**
  * @@Param [topic, value]
  * @[Java]Param [topic, value]
  * @@description 省略key推送消息
  * @@author luyj
  * @@date 2020/3/16 上午10:07
  * @@return _root_.java.util.concurrent.Future[_root_.org.apache.kafka.clients.producer.RecordMetadata]
  * @[java]return java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata>
  */
  def send(topic: String, value: V) =
    producer.send(new ProducerRecord[K, V](topic, value))
}

object KafkaSink{
  import scala.collection.JavaConversions._

  def apply[K, V](config: Map[String, Object]) ={
    //创建生成生产者的函数
    val createProducerFunc = () => {
      val producer: KafkaProducer[K, V] = new KafkaProducer[K, V](config)
      sys.addShutdownHook{
        // Ensure that, on executor JVM shutdown, the Kafka producer sends
        // any buffered messages to Kafka before shutting down.
        producer.close()
      }
      producer
    }
    new KafkaSink[K, V](createProducerFunc)
  }

  def apply[K, V](config: java.util.Properties): KafkaSink[K, V] = apply(config.toMap)
}
