package com.hailian.scala.hbase.org.custom.spark.sql.hbase

import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Copyright (C), 2015-2019, 乐信云科技有限公司
  * FileName: InsertHbaseRelation
  * Author: Luyj
  * Date: 2020/3/18 下午5:22
  * Description:
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  * Luyj              2020/3/18 下午5:22    v1.0              创建 
  */
private[hbase] case class InsertHbaseRelation(
                                             dataFrame: DataFrame,
                                             parameters: Map[String, String]
                                             )(@transient val sqlContext: SQLContext)
extends BaseRelation with InsertableRelation{
  override def schema: StructType = dataFrame.schema

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val tablename: String = parameters.getOrElse("hbase.table.name", sys.error("表没有配置"))
    data.writeToHbase(tablename, parameters)
  }
}
