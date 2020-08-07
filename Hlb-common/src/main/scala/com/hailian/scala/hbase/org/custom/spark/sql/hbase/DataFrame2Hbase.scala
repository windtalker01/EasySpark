package com.hailian.scala.hbase.org.custom.spark.sql.hbase

import com.hailian.scala.utils.GlobalConfigUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.hbase.SerializableConfiguration
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StructField

import scala.collection.immutable.HashMap
/**
  * Copyright (C), 2015-2019, 乐信云科技有限公司
  * FileName: DataFrame2Hbase
  * Author: Luyj
  * Date: 2020/3/18 下午2:50
  * Description:
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  * Luyj              2020/3/18 下午2:50    v1.0              创建 
  */
class DataFrame2Hbase(data: DataFrame) extends Serializable {
  def writeToHbase(tableName: String,
                   options: Map[String, String] = new HashMap[String, String]) = {
    //1 获取rowkey
    val rowkey: String = options.getOrElse("hbase.table.rowkey", sys.error("没有指定rowkey!"))
    //2 校验rowkey，检查作为rowkey的列是否在列里是否存在
//    require(data.schema.fields.map(_.name).contains(rowkey), s"不存在rowkey指定的列：${rowkey}") //也可以
    require(data.schema.fieldNames.contains(rowkey), s"不存在rowkey指定的列：${rowkey}")
    //3 准备要插入的列和rowkey
    val fields: Array[StructField] = data.schema.toArray
//      .sortBy(_.name)
    val zipData: Array[(StructField, Int)] = fields.zipWithIndex //是否应该在这个地方进行排序
    val rowkeyField: (StructField, Int) = zipData.filter(_._1.name == rowkey).head
    val columnFields: Array[(StructField, Int)] = zipData.filter(_._1.name != rowkey)
    val squareRowkey: Row => Any = Resolver.squareRowkey(rowkeyField)
    //4 封装数据到put或bulk
    val squareColumns: Array[(Put, Row, String) => Unit] = columnFields.map(line => Resolver.squareColumns(line))
    val squareColumns2bulk: Array[(Array[Byte], Row, String) => (ImmutableBytesWritable, KeyValue)] = columnFields.map(line => Resolver.squareColumns2bulk(line))
    //5 构建一个hbase的conf

    val hbaseconf: SerializableConfiguration = {
      val config: Configuration = HBaseConfiguration.create()
      config.set("hbase.zookeeper.quorum", GlobalConfigUtils.getProp("hbase.zookeeper.quorum"))
      new SerializableConfiguration(config)
    }
    val hbaseconfig: Configuration = hbaseconf.value

    //获取列族和分区数
    val columnFamily: String = options.getOrElse("hbase.table.columnFamily", "MM")
    val regionNum: Int = options.getOrElse("hbase.table.regionNum", -1).toString.toInt

    val connection: Connection = ConnectionFactory.createConnection(hbaseconfig)
    val admin: Admin = connection.getAdmin
//    if (admin.tableExists(TableName.valueOf(tableName)) != null) {
//      //创建表,生产上保证表已经存在，
//    }
    if (StringUtils.isBlank(hbaseconfig.get("mapreduce.output.fileoutputformat.outputdir"))) {
      hbaseconfig.set("mapreduce.output.fileoutputformat.outputdir", "/tmp/hbase")
    }
    //6 构建正式写hfile的操作
    val jobConf: JobConf = new JobConf(hbaseconfig, this.getClass)
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    //7 指定输出格式
    val job: Job = Job.getInstance(jobConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    //8 根据写入模式，执行写入
    /*
    单条：saveAsNewAPIHadoopDataset ---> RDD[(ImmutableBytesWritable, Put)]
    bulkload: saveAsNewAPIHadoopHfile ---> RDD[(ImmutableBytesWritable, KeyValue)]
    **/
    data.foreach(row => {
      println("df数据：")
      println(row.get(0))
      println(row.get(1))
      println(row.get(2))
    })
    val rdd: RDD[Row] = data.rdd
    rdd.foreach(row => {
      println("rdd数据：")
      println(row.get(0))
      println(row.get(1))
      println(row.get(2))
    })
    options.getOrElse("hbase.engable.bulkload", "true") match {
      case "true" => //bulkload: client -->hdfs(hfile) -->regionServer(加载到region或表)
        val tmpPath = s"/tmp/hbase/bulkload/${tableName}/${System.currentTimeMillis()}"//临时存储路径
        def readyBulkload(row: Row) = {
          val key: String = squareRowkey(row).toString
          val rk: Array[Byte] = Bytes.toBytes(key)
          squareColumns2bulk.map(line => line(rk, row, columnFamily))
        }
        //将rdd落到hfile文件 -->HDFS
        rdd.flatMap(readyBulkload).sortBy(_._1, true).saveAsNewAPIHadoopFile(
          tmpPath,
          classOf[ImmutableBytesWritable],
          classOf[KeyValue],
          classOf[HFileOutputFormat2],
          job.getConfiguration
        )
        //dobulkload 将hfile里的数据对应到表
        val loadIncrementalHFiles: LoadIncrementalHFiles = new LoadIncrementalHFiles(hbaseconfig)
        loadIncrementalHFiles.doBulkLoad(new Path(tmpPath), new HTable(hbaseconfig, tableName))
      case _ => //使用put写入
        def readyPut(row: Row) ={
          val key: String = squareRowkey(row).toString
          val rk: Array[Byte] = Bytes.toBytes(key)
          val put = new Put(rk)
          squareColumns.map(line => line(put, row, columnFamily))
          (new ImmutableBytesWritable, put)
        }
        rdd.map(readyPut).saveAsNewAPIHadoopDataset(job.getConfiguration)
    }
  }

}










































