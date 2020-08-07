package com.hailian.scala.hbase.hutils

import java.io.IOException
import java.lang.reflect.{Field, Type}

import com.hailian.scala.utils.Logging
import org.apache.commons.beanutils.BeanUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * Copyright (C), 2015-2019, 乐信云科技有限公司
  * FileName: HbaseUtil
  * Author: Luyj
  * Date: 2020/3/11 下午3:29
  * Description:
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  * Luyj              2020/3/11 下午3:29    v1.0              创建 
  */
object HbaseUtil extends Serializable with Logging{
  val log: Logger = LoggerFactory.getLogger(this.getClass)
  // hbase的配置信息
  var zookeeperClientport: String = _
  var zookeeperAddress: String = _
  var rpctimeout:String = _
  var hbase_client_scanner_timeout:String = rpctimeout
  var connection: Connection = _ // HBase 连接类
  var admin: Admin = _

  /**
  * @Param [_zookeeperAddress, _zookeeperClientPort, _rpcTimeOut, _hbase_client_scanner_timeout] //参数
  * @[Java]Param [_zookeeperAddress, _zookeeperClientPort, _rpcTimeOut, _hbase_client_scanner_timeout]//java参数
  * @description  初始化hbase连接 //描述
  * @author luyj //作者
  * @date 2020/3/11 下午3:38 //时间
  * @return Unit  //返回值
  * @[java]return void //java返回值
  */
  def init(_zookeeperAddress: String,
           _zookeeperClientPort: String,
           _rpcTimeOut: String = "3600000",
           _hbase_client_scanner_timeout: String = "3600000") = {
    try {
      zookeeperAddress = _zookeeperAddress
      zookeeperClientport = _zookeeperClientPort
      rpctimeout = _rpcTimeOut
      hbase_client_scanner_timeout = _hbase_client_scanner_timeout
      val conf: Configuration = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.property.clientPort", zookeeperClientport)//zk端口
      conf.set("hbase.zookeeper.quorum", zookeeperAddress)//zk地址 ip1,ip2...
      conf.set("hbase.rpc.timeout", rpctimeout)//RPC超时时间
      conf.set("hbase.client.scanner.timeout.period", hbase_client_scanner_timeout)//扫描表超时时间
      if (connection == null || connection.isClosed || connection.isAborted) {
        connection = ConnectionFactory.createConnection(conf)
        admin = connection.getAdmin
      }else{
        println("************** again use Hbase;")
      }
    } catch {
      case e:Exception => e.printStackTrace()
        if(connection != null) connection.close()
        if(admin != null) admin.close()
    }
  }

  /**
  * @Param  //参数
  * @[Java]Param []//java参数
  * @description 重新连接   //描述
  * @author luyj //作者
  * @date 2020/3/11 下午3:39 //时间
  * @return Unit  //返回值
  * @[java]return void //java返回值
  */
  def reconnect(): Unit = {
    try {
      var conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.property.clientPort", zookeeperClientport)//zk端口
      conf.set("hbase.zookeeper.quorum", zookeeperAddress)//zk地址 ip1,ip2...
      conf.set("hbase.rpc.timeout", rpctimeout)//RPC超时时间
      conf.set("hbase.client.scanner.timeout.period", hbase_client_scanner_timeout)//扫描表超时时间
      if (connection == null || connection.isClosed || connection.isAborted) {
        connection = ConnectionFactory.createConnection(conf)
        admin = connection.getAdmin
      }
    }
    catch {
      case e: Exception => {
        print(e.printStackTrace())
        log.error(e.toString)
      }
    }
    finally {
    }

  }

  def createHbaseTable(tableName: String, columnFamilys: Array[String]): Int ={
    try {
      val tName: TableName = TableName.valueOf(tableName)//根据表名字符串构建TableName
      if (!(admin.tableExists(tName))) {//检查表不存在时，创建表
        val descriptor: HTableDescriptor = new HTableDescriptor(tName)//创建表描述
        for (elem <- columnFamilys) {//根据列簇列表在表描述上构建列簇
          descriptor.addFamily(new HColumnDescriptor(elem))
        }
        admin.createTable(descriptor)
        println("create successful!!")
      }
      return 1
    } catch {
      case e:Exception =>{
        println(e.printStackTrace())
        log.error(e.toString)
        return 0
      }
    }
  }

  /**
  * @Param [tableName, family, key, clazz]
  * @[Java]Param [tableName, family, key, clazz]
  * @description 根据rowkey查询hbase并返回bean
  * @author luyj
  * @date 2020/3/17 上午10:22
  * @return Any
  * @[java]return java.lang.Object
  */
  def getBeanByKey[T](tableName: String, family: String, key: String, clazz: Class[T]) = {

    val bean: T = clazz.newInstance()
    var table: Table = null
    try{
      val tabName: TableName = TableName.valueOf(tableName)//构建表名
      table = connection.getTable(tabName)//连接表
      val g: Get = new Get(key.getBytes())//根据rowkey构建get
      val result: Result = table.get(g)//根据rowkey返回结果
      val fields: Array[Field] = clazz.getDeclaredFields //获取传入的类的属性
      var dataMap: Map[String, Any] = Map[String, Any]()

      for (elem <- fields) {
        val column: String = elem.getName
        val colValue = Bytes.toString(result.getValue(family.getBytes(), column.getBytes))
        dataMap += (column -> colValue)
      }
      BeanUtils.populate(bean, dataMap.asJava)
    }catch{
      case e: Exception => println(e.printStackTrace())
    }
    bean
  }

  /**
  * @Param [colVal, colType]
  * @[Java]Param [colVal, colType]
  * @description 根据对应的bean的属性的数据类型获取值,BeanUtils会自动转，暂时不需要
  * @author luyj
  * @date 2020/3/17 下午1:37
  * @return Any
  * @[java]return java.lang.Object
  */
  def valueWithType(colVal:Array[Byte], colType:String) = {
    val colValue = colType match {
      case "class java.lang.String" | "string" => Bytes.toString(colVal)
      case "class java.lang.Integer"| "int" => Integer.valueOf(Bytes.toString(colVal))
      case "class java.lang.Double" | "double" => java.lang.Double.valueOf(Bytes.toString(colVal))
      case "class java.lang.Float" | "float" => java.lang.Float.valueOf(Bytes.toString(colVal))
      case "class java.lang.Short" | "short" => java.lang.Short.valueOf(Bytes.toString(colVal))
      case "class java.lang.Boolean" | "boolean" => java.lang.Boolean.valueOf(Bytes.toString(colVal))
    }
    colValue
  }

  /**
    * @Param [connection hbase连接, tablename hbase表名称, rowkeyCol 作为rowkey的列名 ,
    *       columnFamily 列簇名, data 数据的数组集合, clazz 数据对应的类]
    * @[Java]Param [connection, tablename, rowkey, columnFamily, data, clazz]
    * @description 多条BEAN数组写入HBASE表
    * @author luyj
    * @date 2020/4/10 下午11:05
    * @return Int
    * @[java]return int
    */
  def insBeanToHbase(tablename: String, rowkeyCol: String,  columnFamily:String, data: Array[Any], clazz: Class[_]):Int ={
    var table:Table = null
    try {
      val tName: TableName = TableName.valueOf(tablename)
      table = connection.getTable(tName)
      val puts: ListBuffer[Put] = collection.mutable.ListBuffer[Put]()
      for (elem <- data) {
        val put: Put = generatePut(rowkeyCol, columnFamily, elem, clazz)
        puts += put
      }
      table.put(puts.asJava)
      println("insert successful!!!")

      return 1
    } catch {
      case e:Exception =>{
        println(e.printStackTrace())
        log.error(e.toString)
        return 0
      }
    }finally {
      if (table != null) table.close()
    }
  }
  /**
    * @Param [connection hbase连接, tablename hbase表名称, rowkeyCol 作为rowkey的列名 ,
    *       columnFamily 列簇名, data 数据的数组集合, clazz 数据对应的类]
    * @[Java]Param [connection, tablename, rowkey, columnFamily, data, clazz]
    * @description 单条BEAN数据写入HBASE表
    * @author luyj
    * @date 2020/4/10 下午11:05
    * @return Int
    * @[java]return int
    */
  def insBeanToHbase(connection: Connection,tablename: String, rowkeyCol: String, columnFamily:String, data: Any, clazz: Class[_]):Int ={
    var table:Table = null
    try {
      val tName: TableName = TableName.valueOf(tablename)
      table = connection.getTable(tName)
      val put: Put = generatePut(rowkeyCol, columnFamily, data, clazz)
      table.put(put)
      println("insert successful!!!")

      return 1
    } catch {
      case e:Exception =>{
        println(e.printStackTrace())
        log.error(e.toString)
        return 0
      }
    }finally {
      if (table != null) table.close()
    }
  }
  /**
    * @Param [rowkey, columnFamily, data, clazz]
    * @[Java]Param [rowkey, columnFamily, data, clazz]
    * @description 根据数据和对应的类，反射生成put
    * @author luyj
    * @date 2020/4/10 下午10:59
    * @return _root_.org.apache.hadoop.hbase.client.Put
    * @[java]return org.apache.hadoop.hbase.client.Put
    */
  def generatePut(rowkeyCol: String, columnFamily:String, data: Any, clazz: Class[_])={

    //从对象中指定的rowkey属性里获取rowkey值
    val rowkeyField: Field = clazz.getDeclaredField(rowkeyCol)
    rowkeyField.setAccessible(true)
    val rowkeyStr: String = rowkeyField.get(data).toString
    val put: Put = new Put(rowkeyStr.getBytes())

    val fields: Array[Field] = clazz.getDeclaredFields
    if (fields != null && fields.size > 0) {
      for (field <- fields) {
        if (field.getName != rowkeyCol) {//过滤掉rowkey列，其它列写入hbase
          field.setAccessible(true)
          val colValue = field.get(data).toString
          if (field != null && colValue != null) {
            put.addColumn(columnFamily.getBytes(), field.getName.getBytes(), colValue.getBytes)
          }
        }
      }
    }
    put

  }

  /**
  * @Param [tablename, family] 
  * @[Java]Param [tablename, family]
  * @description 
  * @author luyj
  * @date 2020/4/24 下午9:09
  * @return Unit
  * @[java]return void
  */
  def scanHbase(tablename: String, family:String) ={
    val userTable: TableName = TableName.valueOf(tablename)
    val table: Table = connection.getTable(userTable)
    val scan = new Scan()
    val scanner: ResultScanner = table.getScanner(scan)
    for(result <- scanner.asScala){
      if (result.size() > 0) {

      }
    }
  }

  /**
    * 关闭连接
    **/
  def close() = {
    if (admin != null) {
      try {
        admin.close()
        println("admin关闭成功!")
      } catch {
        case e: IOException => {
          println("admin关闭失败!")
          log.error(e.toString())
        }
      }
    }
    if (connection != null) {
      try {
        connection.close()
        println("关闭成功!")
      } catch {
        case e: IOException => {
          println("关闭失败!")
          log.error(e.toString())
        }
      }
    }
  }

}


























