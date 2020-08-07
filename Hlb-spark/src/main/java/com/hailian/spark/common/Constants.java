package com.hailian.spark.common;

import com.alibaba.fastjson.serializer.SerializerFeature;


/**
 * 常量接口，用于定义常用的常量信息
 */
public interface Constants {

    SerializerFeature SERIALIZER_FEATURE = SerializerFeature.PrettyFormat;
    // RPC 超时时间
    String RPC_TIME_OUT = "rpc.time.out";

    public static final String SPLIT_CHARACTER = ",";
    public static final String UTF8 = "UTF-8";
    public static final String CONNECTION_TIMEOUT = "session.time.out";
    public static final int CONNECTION_SO_TIMEOUT = 30000;
    public static final String SPARK_WORKER_TIMEOUT="500";
    public static final String SPARK_RPC_ASKTIMEOUT="600s";
    public static final String SPARK_TASK_MAXFAILURES="5";
    public static final String SPARK_DRIVER_ALLOWMUTILPLECONTEXT="true";
    public static final String ENABLE_SEND_EMAIL_ON_TASK_FAIL="true";
    public static final String SPARK_BUFFER_PAGESIZE="16m";
    public static final String SPARK_STREAMING_BACKPRESSURE_ENABLED="true";
    public static final String SPARK_STREAMING_BACKPRESSURE_INITIALRATE="2";
    public static final String SPARK_STREAMING_BACKPRESSURE_PID_MINRATE="2";
    public static final String SPARK_SPECULATION_BOOL="true";
    public static final String SPARK_SPECULATION_INTERVAL="300";
    public static final String SPARK_SPECULATION_QUANTILE="0.9";
    public static final String SPARK_STREAMING_KAFKA_MAXRATEPERPARTITION_LOCAL="1";
    public static final String SPARK_SQL_SHUFFLE_PARTITION="3";

}
