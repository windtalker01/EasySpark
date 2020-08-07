package com.hailian.scala.elasticsearch

import org.apache.http.HttpHost
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.RestClientBuilder.{HttpClientConfigCallback, RequestConfigCallback}
import org.elasticsearch.client.{RestClient, RestClientBuilder}
//import org.elasticsearch.client.{RestClient, RestClientBuilder, RestHighLevelClient}
/**
  * Copyright (C), 2015-2019, 乐信云科技有限公司
  * FileName: RESTConnectionPool
  * Author: Luyj
  * Date: 2020/3/22 下午7:40
  * Description: 这个是外援提供的连接池的程序，适合spring MVC,但是不适合spark应用，
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  * Luyj              2020/3/22 下午7:40    v1.0              创建 
  */
class ESRestConnectionPool(esNodesArray: Array[HttpHost]) {
  private val uniqueConnectTimeConfig = true //是否可以设置超时参数开关
  private val uniqueConnectNumConfig = true //是否设置连接数开关
//  private var restHighLevelClient: RestHighLevelClient = _
  private var restClient: RestClient = _ //此处不是static的，每个实例都会初始一个连接池。

  private val CONNECT_TIME_OUT = 60000
  private val SOCKET_TIME_OUT = 60000
  private val CONNECTION_REQUEST_TIME_OUT = 500
  private val MAX_RETRY_TIMEOUT_MILLIS = 5*60*1000
  private val MAX_CONNECT_NUM = 10  //原来是100，现在改成10连接池的连接的个数
  private val MAX_CONNECT_PER_ROUTE = 10 //每个route的连接数，不太清楚作用
  init(esNodesArray) //实例化对象的时候初始化连接池

  private def init(esnodesarray:Array[HttpHost])={
    val builder: RestClientBuilder = RestClient.builder(esNodesArray: _*)
    if (uniqueConnectTimeConfig) {
      setConnectTimeOutConfig(builder)
    }
    if (uniqueConnectNumConfig) {
      setMutiConnectConfig(builder)
    }
    restClient = builder.build()
  }

  /**
  * @Param [builder]
  * @[Java]Param [builder]
  * @description 异步http client 的连接延时配置
  * @author luyj
  * @date 2020/3/22 下午8:38
  * @return _root_.org.elasticsearch.client.RestClientBuilder
  * @[java]return org.elasticsearch.client.RestClientBuilder
  */
  private def setConnectTimeOutConfig(builder: RestClientBuilder)={
    builder.setRequestConfigCallback(new RequestConfigCallback() {
      override def customizeRequestConfig(requestConfigBuilder: RequestConfig.Builder): RequestConfig.Builder = {
        requestConfigBuilder.setConnectTimeout(CONNECT_TIME_OUT)
        requestConfigBuilder.setSocketTimeout(SOCKET_TIME_OUT)
        requestConfigBuilder.setConnectionRequestTimeout(CONNECTION_REQUEST_TIME_OUT)
        requestConfigBuilder
      }
    }).setMaxRetryTimeoutMillis(MAX_RETRY_TIMEOUT_MILLIS)
  }
  /**
  * @Param [builder]
  * @[Java]Param [builder]
  * @description 异步http client 的连接数配置
  * @author luyj
  * @date 2020/3/22 下午8:37
  * @return _root_.org.elasticsearch.client.RestClientBuilder
  * @[java]return org.elasticsearch.client.RestClientBuilder
  */
  private def setMutiConnectConfig(builder: RestClientBuilder) ={
    builder.setHttpClientConfigCallback(new HttpClientConfigCallback() {
      override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = {
        httpClientBuilder.setMaxConnTotal(MAX_CONNECT_NUM)
        httpClientBuilder.setMaxConnPerRoute(MAX_CONNECT_PER_ROUTE)
        httpClientBuilder
      }
    })
  }

  def getClient = {
    restClient
  }
  def close() = {
    if (null != restClient) {
      restClient.close()
    }
  }


}













