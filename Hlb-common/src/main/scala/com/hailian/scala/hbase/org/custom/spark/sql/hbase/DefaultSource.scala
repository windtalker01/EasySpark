package com.hailian.scala.hbase.org.custom.spark.sql.hbase

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider}

/**
  * Copyright (C), 2015-2019, 乐信云科技有限公司
  * FileName: DefaultSource
  * Author: Luyj
  * Date: 2020/3/18 下午2:47
  * Description:
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  * Luyj              2020/3/18 下午2:47    v1.0              创建 
  */
class DefaultSource extends RelationProvider with CreatableRelationProvider{
  //hbase读取
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    HbaseRelation(parameters)(sqlContext)
  }

  //hbase写入
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val insertHbaseRelation: InsertHbaseRelation = new InsertHbaseRelation(data, parameters)(sqlContext)
    insertHbaseRelation.insert(data, false)
    insertHbaseRelation
  }
}
