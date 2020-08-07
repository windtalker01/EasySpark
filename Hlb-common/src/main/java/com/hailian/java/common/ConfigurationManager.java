package com.hailian.java.common;

import java.io.InputStream;
import java.util.Properties;

public class ConfigurationManager {

    private static Properties prop = new Properties();
   static {
        try {
            InputStream in = ConfigurationManager
                    .class
                    .getClassLoader()
                    .getResourceAsStream("my.properties");// 指定要读取的配置文件名称
            prop.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取指定 key 对应的 value
     * 其实就是通过调用本方法传入一个 key 然后返回传入 key 的对应的 value
     * @param key
     * @return
     */
    public static String getProperty(String key) { return (String) prop.getProperty(key); }
    /***
    * @Param  //参数
    * @[Java]Param [key]//java参数
    * @description   //描述
    * @author luyj //作者
    * @date 2020/3/4 下午4:41 //时间
    * @return   //返回值
    * @[java]return java.lang.Integer //java返回值
    */
    public static Integer getInteger(String key) {

        String value = getProperty(key);
        try {
            return Integer.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
    /***
    * @Param  //参数
    * @[Java]Param [key]//java参数
    * @description   //描述
    * @author luyj //作者
    * @date 2020/3/4 下午4:40 //时间
    * @return   //返回值
    * @[java]return java.lang.Boolean //java返回值
    */
    public static Boolean getBoolean(String key) {
        String value = getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
    /***
    * @Param  //参数
    * @[Java]Param [key]//java参数
    * @description   //描述
    * @author luyj //作者
    * @date 2020/3/4 下午4:40 //时间
    * @return   //返回值
    * @[java]return java.lang.Long //java返回值
    */
    public static Long getLong(String key) {
        String value = getProperty(key);
        try {
            return Long.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    /***
    * @Param  //参数
    * @[Java]Param [key]//java参数
    * @description   //描述
    * @author luyj //作者
    * @date 2020/3/4 下午4:40 //时间
    * @return   //返回值
    * @[java]return java.lang.String //java返回值
    */
    public static String getString(String key) {
        String value = getProperty(key);
        try {
            return String.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
