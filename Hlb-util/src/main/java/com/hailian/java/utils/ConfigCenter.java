package com.hailian.java.utils;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import com.hailian.scala.utils.GlobalConfigUtils;
import org.ho.yaml.Yaml;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 海联软件大数据
 * FileName: ConfigCenter
 * Author: Luyj
 * Date: 2020/5/5 下午6:38
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 * Luyj              2020/5/5 下午6:38    v1.0              创建
 */
public class ConfigCenter extends Logging {
    private String address;
    private String group;
    private String nameSpace;
    public ConfigCenter(String _address, String _group, String _nameSpace){
        this.address = _address;
        this.group = _group;
        this.nameSpace = _nameSpace;
    }
    /**
    * @@Param
    * @@[Java]Param [fileName]
    * @@description 根据运行脚本的参数获取配置信息
    * @@author luyj
    * @@date 2020/5/5 下午9:27
    * @@return
    * @@[java]return java.util.Map<java.lang.String,java.lang.String>
    */
    public  HashMap<String, Object>  readFile(String fileName) throws FileNotFoundException {
//        File dumpFile=new File(System.getProperty("CONFIG_HOME") + fileName);
        File dumpFile=new File( fileName);
        Map<String,String> paramMap =Yaml.loadType(dumpFile, HashMap.class);
        HashMap hashMap = Yaml.loadType(dumpFile, HashMap.class);
//        for(Object key:father.keySet()){
//            System.out.println(key+":\t"+father.get(key).toString());
//        }
        System.out.println(hashMap);
        return hashMap;
    }

    /**
    * @@Param
    * @@[Java]Param [fileName]
    * @@description
    * @@author luyj
    * @@date 2020/5/17 下午10:31
    * @@return
    * @@[java]return java.util.HashMap<java.lang.String,java.lang.Object>
    */
    public  HashMap<String, Object>  getConfig(String fileName) throws IOException, NacosException {
        downLoadFromConfigCenter(fileName);
        String dumpFile=System.getProperty("user.dir")+"/" + fileName;
        return readFile(dumpFile);
    }

    /**
    * @@Param
    * @@[Java]Param [fileName]
    * @@description 从配置中心下载配置文件
    * @@author luyj
    * @@date 2020/5/7 下午4:26
    * @@return
    * @@[java]return void
    */
    public  void downLoadFromConfigCenter(String fileName) throws NacosException, IOException {
        String serverAddr = this.address;
        String dataId = fileName;
        String group = this.group;
        String namespace = this.nameSpace;
        Properties properties = new Properties();
        properties.put(PropertyKeyConst.NAMESPACE, namespace);
        properties.put(PropertyKeyConst.SERVER_ADDR, serverAddr);
        ConfigService configService = NacosFactory.createConfigService(properties);
        String content = configService.getConfig(dataId, group, 5000);
//        System.out.println(content);
        if (content != null) {
            writeToFile(content, fileName);
            info("update config info ");
        }else{
            error("config info pull from config center failed!");
        }

    }

    /**
    * @@Param
    * @@[Java]Param [content, fileName]
    * @@description 将下载的配置内容写入到本地的当前目录
    * @@author luyj
    * @@date 2020/5/7 下午4:28
    * @@return
    * @@[java]return void
    */
    public  void writeToFile(String content, String fileName) throws IOException {

        String path = System.getProperty("user.dir");
        System.out.println(path);
        File f=new File(path+"/"+fileName);
        //有一个输出流
        FileOutputStream fos=new FileOutputStream(f);
        byte[] bytes = content.getBytes();//
        for(int i=0;i<=bytes.length-1;i++){
            fos.write(bytes[i]);
        }
        //关闭流
        fos.close();
    }

    public static void main(String[] args) throws NacosException, InterruptedException, IOException {
//        System.out.println(GlobalConfigUtils.getProp("nacos.server.address"));
//        String serverAddr = "114.116.70.28:8848";
        String dataId = "StreamingServiceOrderMainV2.yml";
//        downLoadFromConfigCenter(dataId);
//        String group = "DEFAULT_GROUP";
//        String namespace = "847b80b1-0e6f-4409-ac09-442efd3b969b";
//        Properties properties = new Properties();
//        properties.put(PropertyKeyConst.NAMESPACE, namespace);
//        properties.put(PropertyKeyConst.SERVER_ADDR, serverAddr);
//        ConfigService configService = NacosFactory.createConfigService(properties);
//        String content = configService.getConfig(dataId, group, 5000);
//        System.out.println(content);
//        configService.addListener(dataId, group, new Listener() {
//            @Override
//            public void receiveConfigInfo(String configInfo) {
//                System.out.println("recieve:" + configInfo);
//            }
//
//            @Override
//            public Executor getExecutor() {
//                return null;
//            }
//        });
//
//        boolean isPublishOk = configService.publishConfig(dataId, group, "content");
//        System.out.println(isPublishOk);
//
//        Thread.sleep(3000);
//        content = configService.getConfig(dataId, group, 5000);
//        System.out.println(content);
//
//        boolean isRemoveOk = configService.removeConfig(dataId, group);
//        System.out.println(isRemoveOk);
//        Thread.sleep(3000);
//
//        content = configService.getConfig(dataId, group, 5000);
//        System.out.println(content);
//        Thread.sleep(300000);
    }
}
