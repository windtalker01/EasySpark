package com.hailian.scala.hbase.org.custom.spark.sql.hbase

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.joda.time.DateTime

/**
  * Copyright (C), 2015-2019, 乐信云科技有限公司
  * FileName: Resolver
  * Author: Luyj
  * Date: 2020/3/17 上午11:12
  * Description:
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  * Luyj              2020/3/17 上午11:12    v1.0              创建 
  */
object Resolver extends Serializable {
  /**
  * @Param [result, resultType]
  * @[Java]Param [result, resultType]
  * @description 根据数据类型解析rowkey
  * @author luyj
  * @date 2020/3/17 上午11:26
  * @return Unit
  * @[java]return void
  */
  private def resolveRowkey[T <: Result, S <: String](result: Result, resultType: String) ={
    val rowkey = resultType match {
      case "String" => result.getRow.map(_.toChar).mkString.toString
      case "Int" => result.getRow.map(_.toChar).mkString.toInt
      case "Double" => result.getRow.map(_.toChar).mkString.toDouble
      case "Float" => result.getRow.map(_.toChar).mkString.toFloat
      case "Long" => result.getRow.map(_.toChar).mkString.toLong
      case "Boolean" => result.getRow.map(_.toChar).mkString.toBoolean
      case "Short" => result.getRow.map(_.toChar).mkString.toShort
    }
  }
  /**
  * @Param [result, columnFamily, columnName, resultType]
  * @[Java]Param [result, columnFamily, columnName, resultType]
  * @description 解析hbase列数据
  * @author luyj
  * @date 2020/3/17 上午11:34
  * @return Any
  * @[java]return java.lang.Object
  */
  private def resolveColumn(result: Result,
                            columnFamily: String,
                            columnName: String,
                            resultType: String) ={
    result.containsColumn(columnFamily.getBytes(), columnName.getBytes) match {
      case true =>
        resultType match {//非String的转换，以下方法可能会报错，需要对应的类型的valueOf函数转换Bytes.toShort 返回为short基本类型
          case "String" => Bytes.toString(result.getValue(columnFamily.getBytes, columnName.getBytes))
          case "Int" => Bytes.toInt(result.getValue(columnFamily.getBytes, columnName.getBytes))
          case "Double" => Bytes.toDouble(result.getValue(columnFamily.getBytes, columnName.getBytes))
          case "Float" => Bytes.toFloat(result.getValue(columnFamily.getBytes, columnName.getBytes))
          case "Long" => Bytes.toLong(result.getValue(columnFamily.getBytes, columnName.getBytes))
          case "Boolean" => Bytes.toBoolean(result.getValue(columnFamily.getBytes, columnName.getBytes))
          case "Short" => Bytes.toShort(result.getValue(columnFamily.getBytes, columnName.getBytes))
        }
      case _ =>
        resultType match {
          case "String" => ""
          case "Int" => 0
          case "Double" => 0.0
          case "Float" => 0.0
          case "Long" => 0L
          case "Boolean" => false
          case "Short" => 0
        }
    }
  }

  /**
  * @Param [hbaseSchemaField, result]
  * @[Java]Param [hbaseSchemaField, result]
  * @description 解析hbase数据
  * @author luyj
  * @date 2020/3/17 下午2:17
  * @return Any
  * @[java]return java.lang.Object
  */
  def resolve(hbaseSchemaField: HbaseSchemaField, result: Result)={
    //cf:field，解析列族和列名
    val fieldArr: Array[String] = hbaseSchemaField.fieldName.split(":", -1)
    val cf: String = fieldArr(0)
    val col: String = fieldArr(1)
    val res = if (cf == null && col == "key") {
      resolveRowkey(result, hbaseSchemaField.fieldType)
    }else{
      resolveColumn(result, cf, col, hbaseSchemaField.fieldType)
    }
    res
  }

  //封装rowkey
  def squareRowkey(dataType:(StructField, Int)):(Row) => Any ={
    val (structField, index) = dataType
    structField.dataType match {
      case StringType => (row: Row) => row.getString(index)
      case IntegerType => (row: Row) => row.getInt(index)
      case DoubleType => (row: Row) => row.getDouble(index)
      case LongType => (row: Row) => row.getLong(index)
      case LongType => (row: Row) => java.lang.Long.valueOf(row.getString(index))
      case FloatType => (row: Row) => row.getFloat(index)
      case BooleanType => (row: Row) => row.getBoolean(index)
      case DateType => (row: Row) => row.getDate(index)
      case TimestampType => (row: Row) => row.getTimestamp(index)
      case BinaryType => (row: Row) => row.getAs[Array[Byte]](index)
      case _ => (row: Row) => row.getString(index)
    }
  }

  //封装非rowkey的列到put
  def squareColumns(dataType:(StructField, Int)):(Put, Row, String) => Unit = {
    val (structField, index) = dataType
    structField.dataType match {
      case StringType =>
        (put: Put, row: Row, cm: String) =>
        if (row.getString(index)== null) {
          put.addColumn(Bytes.toBytes(cm), Bytes.toBytes(structField.name), Bytes.toBytes(""))
        }else{
          put.addColumn(Bytes.toBytes(cm), Bytes.toBytes(structField.name), Bytes.toBytes(row.getString(index)))
        }
      case IntegerType =>
        (put: Put, row: Row, cm: String) =>
        put.addColumn(Bytes.toBytes(cm), Bytes.toBytes(structField.name), Bytes.toBytes(row.getInt(index)))
      case DoubleType =>
        (put: Put, row: Row, cm: String) =>
          put.addColumn(Bytes.toBytes(cm), Bytes.toBytes(structField.name), Bytes.toBytes(row.getDouble(index)))
      case LongType =>
        (put: Put, row: Row, cm: String) =>
          put.addColumn(Bytes.toBytes(cm), Bytes.toBytes(structField.name), Bytes.toBytes(row.getLong(index)))
      case FloatType =>
        (put: Put, row: Row, cm: String) =>
          put.addColumn(Bytes.toBytes(cm), Bytes.toBytes(structField.name), Bytes.toBytes(row.getFloat(index)))
      case BooleanType =>
        (put: Put, row: Row, cm: String) =>
          put.addColumn(Bytes.toBytes(cm), Bytes.toBytes(structField.name), Bytes.toBytes(row.getBoolean(index)))
      case DateType =>
        (put: Put, row: Row, cm: String) =>
          put.addColumn(Bytes.toBytes(cm), Bytes.toBytes(structField.name), Bytes.toBytes(new DateTime(row.getDate(index)).getMillis))
      case TimestampType =>
        (put: Put, row: Row, cm: String) =>
          put.addColumn(Bytes.toBytes(cm), Bytes.toBytes(structField.name), Bytes.toBytes(new DateTime(row.getTimestamp(index)).getMillis))
      case BinaryType =>
        (put: Put, row: Row, cm: String) =>
          put.addColumn(Bytes.toBytes(cm), Bytes.toBytes(structField.name), row.getAs[Array[Byte]](index))
      case _ =>
        (put: Put, row: Row, cm: String) =>
          put.addColumn(Bytes.toBytes(cm), Bytes.toBytes(structField.name), Bytes.toBytes(row.getString(index)))
    }
  }
  //封装非rowkey列到bulk
  def squareColumns2bulk(dataType: (StructField, Int)):(Array[Byte], Row, String) => (ImmutableBytesWritable, KeyValue) ={
    val (structField, index) = dataType
    structField.dataType match {
      case StringType =>
        (rk: Array[Byte], row: Row, cm: String) =>
          if (row.getString(index)== null) {
            (new ImmutableBytesWritable(rk), new KeyValue(rk, cm.getBytes, structField.name.getBytes,Bytes.toBytes(row.getString(index))))
          }else{
            (new ImmutableBytesWritable(rk), new KeyValue(rk, cm.getBytes, structField.name.getBytes, Bytes.toBytes(row.getString(index))))
          }
      case IntegerType =>
        (rk: Array[Byte], row: Row, cm: String) =>
          (new ImmutableBytesWritable(rk), new KeyValue(rk, cm.getBytes, structField.name.getBytes, Bytes.toBytes(row.getInt(index))))
      case DoubleType =>
        (rk: Array[Byte], row: Row, cm: String) =>
          (new ImmutableBytesWritable(rk), new KeyValue(rk, cm.getBytes, structField.name.getBytes,Bytes.toBytes(row.getDouble(index))))
      case LongType =>
        (rk: Array[Byte], row: Row, cm: String) =>
          (new ImmutableBytesWritable(rk), new KeyValue(rk, cm.getBytes, structField.name.getBytes, Bytes.toBytes(row.getLong(index))))
      case FloatType =>
        (rk: Array[Byte], row: Row, cm: String) =>
          (new ImmutableBytesWritable(rk), new KeyValue(rk, cm.getBytes, structField.name.getBytes, Bytes.toBytes(row.getFloat(index))))
      case BooleanType =>
        (rk: Array[Byte], row: Row, cm: String) =>
          (new ImmutableBytesWritable(rk), new KeyValue(rk, cm.getBytes, structField.name.getBytes, Bytes.toBytes(row.getBoolean(index))))
      case DateType =>
        (rk: Array[Byte], row: Row, cm: String) =>
          (new ImmutableBytesWritable(rk), new KeyValue(rk, cm.getBytes, structField.name.getBytes, Bytes.toBytes(new DateTime(row.getDate(index)).getMillis)))
      case TimestampType =>
        (rk: Array[Byte], row: Row, cm: String) =>
          (new ImmutableBytesWritable(rk), new KeyValue(rk, cm.getBytes, structField.name.getBytes, Bytes.toBytes(new DateTime(row.getDate(index)).getMillis)))
      case BinaryType =>
        (rk: Array[Byte], row: Row, cm: String) =>
          (new ImmutableBytesWritable(rk), new KeyValue(rk, cm.getBytes, structField.name.getBytes, row.getAs[Array[Byte]](index)))
      case _ =>
        (rk: Array[Byte], row: Row, cm: String) =>
          (new ImmutableBytesWritable(rk), new KeyValue(rk, cm.getBytes, structField.name.getBytes, Bytes.toBytes(row.getString(index))))
    }
  }


}
