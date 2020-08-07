package com.hailian.scala.elasticsearch

import java.util

import com.hailian.scala.utils.Logging
import org.apache.http.HttpHost
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.elasticsearch.client.{Response, RestClient}

/**
  * Copyright (C), 2015-2019, 乐信云科技有限公司
  * FileName: ESConnect
  * Author: Luyj
  * Date: 2020/3/22 下午10:02
  * Description:
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  * Luyj              2020/3/22 下午10:02    v1.0              创建 
  */
object ESRestConnect extends Serializable with Logging {
  @volatile var pool: ESRestConnectionPool = null
  /**
  * @Param [esNodesArray]
  * @[Java]Param [esNodesArray]
  * @description 初始化es连接池
  * @author luyj
  * @date 2020/3/23 下午1:35
  * @return Unit
  * @[java]return void
  */
  def init(esNodesArray: Array[HttpHost]) = {
    if (pool == null) {
      synchronized {//减少锁等待，只有在出现连接为null时，才进行初始化连接池
        if (pool == null) {
          try {
            pool = new ESRestConnectionPool(esNodesArray)
          } catch {
            case e:Exception =>
              if (pool != null) {pool.close()}
              println(e.printStackTrace())
          }
        }
      }
    }else{
      println("************** again use ES;")
    }
  }
  /**
  * @Param [tableName, tableType, sourceId, newJson]
  * @[Java]Param [tableName, tableType, sourceId, newJson]
  * @description 使用json数据，根据_id往es里写数据
  * @author luyj
  * @date 2020/3/23 下午1:34
  * @return Unit
  * @[java]return void
  */
  def indexdata(tableName: String,
                tableType:String,
                sourceId: String,
                newJson: String)={
    val client: RestClient = pool.getClient
    val params: util.Map[String, String] = java.util.Collections.singletonMap("pretty", "true")
    val entity: NStringEntity = new NStringEntity(newJson, ContentType.APPLICATION_JSON)
    val response: Response = client.performRequest("POST", raw"/$tableName/$tableType/$sourceId", params, entity)
    debug(s"========indexDataBylowRest=======response::::$response")
  }
}
