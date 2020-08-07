import java.lang

import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConversions._
import scala.collection.mutable
/**
  * 海联软件大数据
  * FileName: ConsumerOffsetTest
  * Author: Luyj
  * Date: 2020/5/27 下午3:45
  * Description:
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  * Luyj              2020/5/27 下午3:45    v1.0              创建 
  */
object ConsumerOffsetTest {
  def main(args: Array[String]): Unit = {
    val topic = "lecc_photo_tag_manual_test"
    val topics = Array("lecc_photo_tag_manual_test","offset_test")
    val kafkaServers = "10.155.20.86:9092"
    val groupId = "offsetGroup008"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val (zkClient, zkConnection) = ZkUtils.createZkClientAndConnection("lx-cs-04:2181,lx-cs-05:2181/kafkauat", 10000, 10000)
    val zkUtils = new ZkUtils(zkClient, zkConnection, false)
    val topicAndPartitionMaps: mutable.Map[String, Seq[Int]] = zkUtils.getPartitionsForTopics(topics)
    topicAndPartitionMaps.foreach(topicPartitions =>{
      val zkGroupTopicDirs: ZKGroupTopicDirs = new ZKGroupTopicDirs(groupId, topicPartitions._1)
      topicPartitions._2.foreach(partition =>{

        val consumer: KafkaConsumer[String, Object] = new KafkaConsumer[String, Object](kafkaParams)
        val topicCllection: List[TopicPartition] = List(new TopicPartition(topicPartitions._1, Integer.valueOf(partition)))
        val topicPart: TopicPartition = new TopicPartition(topicPartitions._1, Integer.valueOf(partition))
        consumer.assign(topicCllection)
//        val avaliableOffset: lang.Long = consumer.beginningOffsets(topicCllection).values().head
        val avaliableOffset: Long = consumer.position(topicCllection.head)
        consumer.close()
        println(s"topicCllection:${topicCllection},avaliableOffset:${avaliableOffset}")

      })
    })
  }

}
