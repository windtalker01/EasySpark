package com.hailian.scala.hbase.org.custom.spark.sql.hbase

import com.hailian.scala.utils.GlobalConfigUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Copyright (C), 2015-2019, 乐信云科技有限公司
  * FileName: HbaseRelation
  * Author: Luyj
  * Date: 2020/3/17 下午2:59
  * Description:
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  * Luyj              2020/3/17 下午2:59    v1.0              创建 
  */
private[hbase] case class HbaseRelation(@transient val hbaseProps: Map[String, String])
                                       (@transient val sqlContext: SQLContext)
extends BaseRelation with TableScan { //实际生产环境中不使用tablescan，全表扫描，速度太慢
  //1 获取hbase表名
  private val hbaseTableName: String = hbaseProps.getOrElse("hbase_table_name", sys.error("获取hbase表名失败！！！"))
  //2 获取查询的rowkey范围
  private val rowRange: String = hbaseProps.getOrElse("hbase.table.rowkey", "->")
  private val rowArr: Array[String] = rowRange.split("->", -1)
  val startRowkey = rowArr(0).trim
  val endRowkey = rowArr(1).trim
  //3 获取配置文件中的hbase schema的字符串
  private val hbaseTableSchema: String = hbaseProps.getOrElse("hbase_table_schema", sys.error("获取hbase schema字符串失败！！！"))
  //4 获取配置文件中的spark schema的字符串
  private val registerTableschema: String = hbaseProps.getOrElse("sparksql_table_schema", sys.error("获取sparksql schema字符串失败！！！"))
  //7 将hbase schema处理成Array[HbaseSchemaField]
  private val tmpHbaseSchemaField: Array[HbaseSchemaField] = extractHbaseSchema(hbaseTableSchema)
  //8 将spark schema处理成Array[RegisterSchemaField]
  private val registerTableFields: Array[RegisterSchemaField] = extractRegisterSchema(registerTableschema)
  //10  将Array[HbaseSchemaField]与Array[RegisterSchemaField]生成拉链mutable.LinkedHashMap[HbaseSchemaField, RegisterSchemaField]
  private val zipSchema: mutable.LinkedHashMap[HbaseSchemaField, RegisterSchemaField] = zipHbaseWithRegister(tmpHbaseSchemaField, registerTableFields)
  //12  根据mutable.LinkedHashMap[HbaseSchemaField, RegisterSchemaField] feed出Array[HbaseSchemaField]的数据类型
  private val finalHbaseSchema: Array[HbaseSchemaField] = feedHbaseTypeWithZip(zipSchema)
  //15  将feed完成的Array[HbaseSchemaField]转换成字符串
  private val searchColumns: String = getHbaseSearchSchema(finalHbaseSchema)
  //16 feed类型后的Array[HbaseSchemaField]与Array[RegisterSchemaField]重新生成拉链
  private val fieldStructFileds: mutable.LinkedHashMap[HbaseSchemaField, RegisterSchemaField] = zipHbaseWithRegister(finalHbaseSchema, registerTableFields)
  //5 定义函数：将hbase schema处理成Array[HbaseSchemaField]
  private def extractHbaseSchema[T <: String] (hbaseTabSchema: String) ={
    hbaseTabSchema.drop(1).dropRight(1).split(",").map(col => HbaseSchemaField(col.trim, ""))
  }
  //6 定义函数：将spark schema处理成Array[RegisterSchemaField]
  private def extractRegisterSchema[T <: String](registerTabSchema: String) ={
    registerTabSchema.drop(1).dropRight(1).split(",").map(col => {
      val strArr: Array[String] = col.trim.split("\\s+")
      RegisterSchemaField(strArr(0).trim, strArr(1).trim)
    })

  }
  //9 定义函数： 将Array[HbaseSchemaField]与Array[RegisterSchemaField]生成拉链mutable.LinkedHashMap[HbaseSchemaField, RegisterSchemaField]
  private def zipHbaseWithRegister[T <: HbaseSchemaField, S <: RegisterSchemaField](hbaseArr: Array[HbaseSchemaField],registerArr: Array[RegisterSchemaField]) ={
    val map = new mutable.LinkedHashMap[HbaseSchemaField, RegisterSchemaField]
    if (hbaseArr.length != registerArr.length) {sys.error("两个scchema不一致")}
    hbaseArr.zip(registerArr).foreach(elem => map.put(elem._1 , elem._2))
    map
  }
  //11 定义函数：根据mutable.LinkedHashMap[HbaseSchemaField, RegisterSchemaField]对应出Array[HbaseSchemaField]的数据类型
  private def feedHbaseTypeWithZip[T <: mutable.LinkedHashMap[HbaseSchemaField, RegisterSchemaField]](zipSchema: mutable.LinkedHashMap[HbaseSchemaField, RegisterSchemaField]) ={
    val finalHbaseSchema: mutable.Iterable[HbaseSchemaField] = zipSchema.map{
      case (hbaseSchema, registerSchema) => hbaseSchema.copy(fieldType = registerSchema.fieldType)
    }
    finalHbaseSchema.toArray
  }
  //13 定义函数：构建查询的hbase字段字符串 cf:field1 cf:field2 cf:field3
  private def getHbaseSearchSchema[T <: HbaseSchemaField](finalHbaseSchema: Array[HbaseSchemaField]) = {
    val str: ArrayBuffer[String] = ArrayBuffer[String]()
    finalHbaseSchema.foreach(field => {
      if (!(isRowkey(field))) {
        str.append(field.fieldName)
      }
    })
    str.mkString(" ")
  }
  //14 定义函数：识别rowkey
  private def isRowkey[T <: HbaseSchemaField](hbaseSchemaField: HbaseSchemaField) = {
    val rowkeyArr: Array[String] = hbaseSchemaField.fieldName.split(":", -1)
    if (rowkeyArr(0) == null && rowkeyArr(1) == "key") true else false
  }

  override def schema: StructType = {
    val fields: Array[StructField] = finalHbaseSchema.map { field =>
      val name: RegisterSchemaField = fieldStructFileds.getOrElse(field, sys.error(s"无法获取到此列：${field}"))
      val relationType = field.fieldType match {
        case "String" => SchemaType(StringType, false)
        case "Int" => SchemaType(IntegerType, false)
        case "Long" => SchemaType(LongType, false)
        case "Double" => SchemaType(DoubleType, false)
      }
      StructField(name.fieldName, relationType.dataType, relationType.nullable)
    }
    StructType(fields)
  }

  override def buildScan(): RDD[Row] = {
    //配置hbase连接的configuration
    val hbaseconf: Configuration = HBaseConfiguration.create()
    hbaseconf.set("hbase.zookeeper.quorum", GlobalConfigUtils.getProp("hbase.zookeeper.quorum"))
    hbaseconf.set(TableInputFormat.INPUT_TABLE, hbaseTableName)
    hbaseconf.set(TableInputFormat.SCAN_COLUMNS, searchColumns)
//    hbaseconf.set(TableInputFormat.SCAN,"""{FILTER=>"ValueFilter(=,'binary:Andy')}"""")
    hbaseconf.set(TableInputFormat.SCAN_ROW_START, startRowkey)
    hbaseconf.set(TableInputFormat.SCAN_ROW_STOP, endRowkey)
    hbaseconf.set(TableInputFormat.SCAN_CACHEDROWS, "10000")
    hbaseconf.set(TableInputFormat.SHUFFLE_MAPS, "1000")

    //通过newAPIHadoopRDD查询
    val hbaseRdd: RDD[(ImmutableBytesWritable, Result)] = sqlContext.sparkContext.newAPIHadoopRDD(
      hbaseconf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    val finalResult: RDD[Row] = hbaseRdd.map(_._2).map(result => {
      val values: ArrayBuffer[Any] = new ArrayBuffer[Any]()
      finalHbaseSchema.foreach(field => {
        values += Resolver.resolve(field, result)
      })
      Row.fromSeq(values)
    })
    finalResult
  }
}

private case class SchemaType(dataType: DataType, nullable: Boolean)

















































