package com.hailian.scala.hbase.org.custom.spark.sql

import org.apache.spark.sql.DataFrame

package object hbase {
  //属性类封装
  abstract class SchemaField extends Serializable
  //sparkschema封装
  case class RegisterSchemaField(fieldName: String, fieldType: String) extends SchemaField with Serializable
  //hbaseschema封装
  case class HbaseSchemaField(fieldName: String, fieldType: String) extends SchemaField with Serializable

  implicit def DFToHbase(data: DataFrame) = {
    new DataFrame2Hbase(data)
  }

}
