package com.hailian.scala.hbase.org.custom.spark.sql.hbase


import com.hailian.scala.utils.GlobalConfigUtils
import org.apache.spark.sql.DataFrame

/**
  * Copyright (C), 2015-2019, 乐信云科技有限公司
  * FileName: DataFrameToHbase
  * Author: Luyj
  * Date: 2020/3/18 下午5:27
  * Description:
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  * Luyj              2020/3/18 下午5:27    v1.0              创建 
  */
object DataFrameToHbase {
  lazy val save = (result: DataFrame, tableName: String, rowkey:String, regionNum: Int, bulkload: Boolean) => {
    result.write.format(GlobalConfigUtils.getProp("custom.hbase.path"))
      .options(Map(
        "hbase.table.name" -> tableName,
        "hbase.table.rowkey" -> rowkey,
        "hbase.table.columnFamily" -> "MM",
        "hbae.table.regionNum" -> s"${regionNum}",
        "hbase.engable.bulkload" -> s"${bulkload}"
      )).save()
  }
}
