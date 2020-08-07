package com.hailian.java.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * 海联软件大数据
 * FileName: MultiESTransportUtils
 * Author: Luyj
 * Date: 2020/4/27 下午5:16
 * Description: 创建多个transport client的静态连接
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 * Luyj              2020/4/27 下午5:16    v1.0              创建
 */
public class MultiESTransportUtils {
    private static final int PORT = 9300;// 端口
    public static TransportClient client;// 创建对象
    public static HashMap<String, TransportClient> clients;//多个transport连接对象

    public MultiESTransportUtils(String esInfo){//C1:lseuat:10.155.20.86:9300,C2:lseuat:10.155.20.86:9300
        HashMap<String, TransportInfoBean> connMap = new HashMap<String, TransportInfoBean>();
        String[] ess = esInfo.split(",");
        for (String s : ess) {
            String[] res = s.split(":");
            TransportInfoBean value = new TransportInfoBean(res[1],res[2],Integer.parseInt(res[3]));
        }
    }
    public MultiESTransportUtils(HashMap<String,TransportInfoBean> connMap){
        Iterator<Map.Entry<String, TransportInfoBean>> iter = connMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, TransportInfoBean> entry = iter.next();
            String key = entry.getKey();
            TransportInfoBean value = entry.getValue();
            int port = value.getPort();
            if(value.getPort() == 0 || "".equals(value.getPort())){//没有提供port则使用默认的port值
                port = PORT;
            }
            TransportClient client = getClient(value.getClusterName(), value.getNodeIP(), port);
            clients.put(key, client);
        }
    }
    /***
     * @Param  //参数
     * @[Java]Param [clusterName, ip]//java参数
     * @description   根据集群名称和IP获取es连接//描述
     * @author luyj //作者
     * @date 2020/3/4 下午5:15 //时间
     * @return   //返回值
     * @[java]return org.elasticsearch.client.transport.TransportClient //java返回值
     */
    public static synchronized TransportClient getClient(String clusterName, String ip){
        TransportClient client = getClient(clusterName, ip, PORT);
        return client;
    }
    /***
     * @Param
     * @[Java]Param [clusterName, ip, PORT]
     * @description 非默认transport端口是生成客户端
     * @author luyj
     * @date 2020/4/9 上午10:25
     * @return
     * @[java]return org.elasticsearch.client.transport.TransportClient
     */
    public static synchronized TransportClient getClient(String clusterName, String ip, int PORT){
        TransportClient client = null;
        try {
            if (client == null) {
                Settings settings = Settings.builder().put("cluster.name", clusterName)
//                        .put("client.transport.ping_timeout",3)
                        .put("transport.connections_per_node.bulk","1")
                        .put("client.transport.sniff", true).build();
                client = new PreBuiltTransportClient(settings);
                // 至少一个，如果设置了"client.transport.sniff"= true 一个就够了，因为添加了自动嗅探配置
                InetSocketTransportAddress inetSocketTransportAddress = new InetSocketTransportAddress(InetAddress.getByName(ip), Integer.valueOf(PORT));
                client.addTransportAddresses(inetSocketTransportAddress);
            }
        } catch (Exception e) {
            System.out.println("TransportClient初期化失败...");
            e.printStackTrace();
            client.close();
        }
        return client;
    }
    /***
     * @Param  //参数
     * @[Java]Param [name]//java参数
     * @description 为集群添加新的节点   //描述
     * @author luyj //作者
     * @date 2020/3/4 下午5:19 //时间
     * @return   //返回值
     * @[java]return void //java返回值
     */
    public static synchronized void addNode(String name){
        try {
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(name), Integer.valueOf(PORT)));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
    /***
     * @Param  //参数
     * @[Java]Param [name]//java参数
     * @description 从集群中删除某个节点  //描述
     * @author luyj //作者
     * @date 2020/3/4 下午5:21 //时间
     * @return   //返回值
     * @[java]return void //java返回值
     */
    public static synchronized void removeNode(String name){
        try {
            client.removeTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(name), Integer.valueOf(PORT)));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public static synchronized void close(){
        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception e) {
            System.out.println("关闭客户端失败....");
            e.printStackTrace();
        }
    }

    /**
     * @return
     * @Param
     * @[Java]Param [index, type, _id, objIter]
     * @description 传入object迭代器数据，不指定_id和写入批次大小，默认批次1000，批量写入数据到es
     * @author luyj
     * @date 2020/4/23 上午11:53
     * @[java]return void
     */
    public synchronized void bulkInsertToES(TransportClient client, String index, String type, Iterator<Object> objIter) {
        bulkInsertToES(client,index, type, objIter, null);
    }

    /**
     * @return
     * @Param
     * @[Java]Param [index, type, objIter, _id]
     * @description 传入object迭代器数据，指定_id,不指定写入批次大小，默认批次1000，批量写入数据到es
     * @author luyj
     * @date 2020/4/24 下午1:08
     * @[java]return void
     */
    public synchronized void bulkInsertToES(TransportClient client, String index, String type, Iterator<Object> objIter, String _id) {
        int batchSize = 1000;
        bulkInsertToES(client,index, type, objIter, _id, batchSize);
    }

    /**
     * @return
     * @Param
     * @[Java]Param [index, type, _id, objIter, batchSize]
     * @description 传入object迭代器数据，设定提交批次和_id的属性名，批量写入数据到es
     * @author luyj
     * @date 2020/4/23 上午11:51
     * @[java]return void
     */
    public synchronized void bulkInsertToES(TransportClient client, String index, String type, Iterator<Object> objIter, String _id, int batchSize) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();//构建批次请求
        int count = 0;//批请求数量计数
        Field colField = null;//指定的主键类属性
        String value = null;//主键值
        while (objIter.hasNext()) {
            Object bean = objIter.next();
            Class<?> clazz = bean.getClass();
            colField = null;
            value = null;

            if (_id != null) {//若指定了主键
                try {
                    colField = clazz.getDeclaredField(_id);
                    colField.setAccessible(true);
                    value = colField.get(bean).toString();
                } catch (NoSuchFieldException e) {
                    System.out.println("指定主键的属性名："+ _id+" 不存在！");
                    e.printStackTrace();
                }catch (Exception e) {
                    System.out.println("获取主键的属性："+ _id+" 的值失败，数据将被丢弃！");
                    e.printStackTrace();
                }
            }

            bulkRequest(bulkRequest, client, index, type, _id, value, bean);
            count += 1;
            if (count % batchSize == 0) {//
                bulkRequest.execute().actionGet();
                bulkRequest = client.prepareBulk();
                count = 0;
            }
        }
        if (count > 0 && bulkRequest.numberOfActions() > 0) {
            bulkRequest.execute().actionGet();
        }

    }
    /**
     * @Param
     * @[Java]Param [bulkRequest, index, type, _id, value, bean]
     * @description 根据是否指定主键，或主键值是否合法，装载请求
     * @author luyj
     * @date 2020/4/24 下午5:15
     * @return
     * @[java]return void
     */
    private synchronized void bulkRequest(BulkRequestBuilder bulkRequest, TransportClient client, String index, String type, String _id, String value,Object bean) {
        if (_id != null) {
            if (value != null && !"".equals(value)) {//如果指定主键，则数据中主键列必须有值，否则抛弃
                bulkRequest.add(client.prepareIndex(index, type, _id).setSource(JSON.toJSONString(bean), XContentType.JSON));
            }
        } else {
            bulkRequest.add(client.prepareIndex(index, type).setSource(JSON.toJSONString(bean), XContentType.JSON));
        }
    }

    /**
     * @return //返回值
     * @Param //参数
     * @[Java]Param [client, index, type, fieldMap]//java参数
     * @description 根据查询条件返回最多10条结果   //描述
     * @author luyj //作者
     * @date 2020/3/11 下午4:55 //时间
     * @[java]return java.util.List<java.lang.String> //java返回值
     */
    public synchronized ArrayList<String> searchTerms(TransportClient client, String index, String type, Map<String, String> fieldMap) {
        ArrayList<String> list = new ArrayList<>();//用于保存查询结果的json数据组
        BoolQueryBuilder bqb = QueryBuilders.boolQuery();//构建ES的bool查询
        for (String key : fieldMap.keySet()) {//根据查询的条件，构建bool查询的must描述
            TermQueryBuilder qb = QueryBuilders.termQuery(key, fieldMap.get(key));
            bqb.must(qb);
        }
        /**
         * The number of search hits to return. Defaults to <tt>10</tt>.
         */
        SearchResponse sr = client.prepareSearch(index).setTypes(type).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(bqb).setSize(10).get();//构建查询
        SearchHits hits = sr.getHits();//执行查询，返回结果
        if (hits.totalHits() < 10) {//查询结果小于10条则，直接返回结果LIST
            for (SearchHit hit : hits) {
                String json = hit.getSourceAsString();
                list.add(json);
            }
        } else {//否则，即超过10条，则通过scroll返回所有记录
            list = searchAllTers(client, index, type, fieldMap);
        }
        return list;
    }

    /**
     * @return //返回值
     * @Param //参数
     * @[Java]Param [client, index, type, fieldMap]//java参数
     * @description 根据条件返回所有的记录   //描述
     * @author luyj //作者
     * @date 2020/3/11 下午5:27 //时间
     * @[java]return java.util.List<java.lang.String> //java返回值
     */
    public synchronized ArrayList<String> searchAllTers(TransportClient client, String index, String type, Map<String, String> fieldMap) {
        ArrayList<String> list = new ArrayList<>();//用于保存查询结果的json数据组
        BoolQueryBuilder bqb = QueryBuilders.boolQuery();//构建ES的bool查询
        for (String key : fieldMap.keySet()) {//根据查询的条件，构建bool查询的must描述
            TermQueryBuilder qb = QueryBuilders.termQuery(key, fieldMap.get(key));
            bqb.must(qb);
        }
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setIndices(index).setTypes(type).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(bqb).setSize(100);
        searchRequestBuilder.setScroll(TimeValue.timeValueMinutes(1));//设置 search context 维护1分钟的有效期
        SearchResponse scrollResp = searchRequestBuilder.execute().actionGet();//获得首次的查询结果
        for (SearchHit searchHit : scrollResp.getHits().getHits()) {
            String json = searchHit.getSourceAsString();
            list.add(json);
        }

        do {
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(TimeValue.timeValueMinutes(1))
                    .execute().actionGet();
            for (SearchHit searchHit : scrollResp.getHits().getHits()) {
                String json = searchHit.getSourceAsString();
                list.add(json);
            }
        } while (scrollResp.getHits().getHits().length != 0);
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollResp.getScrollId());
        client.clearScroll(clearScrollRequest).actionGet();

        return list;

    }

    /***
     * @Param
     * @[Java]Param [client, index, type, fieldMap]
     * @description 查询分页
     * @author luyj
     * @date 2020/3/30 下午10:57
     * @return
     * @[java]return java.util.List<java.lang.String>
     */
    public synchronized List<String> searchTermsPages(TransportClient client,
                                                      String index,
            String type,
            Map<String, String> fieldMap,
            Integer from,
            Integer pageSize) {
        ArrayList<String> list = new ArrayList<>();//用于保存查询结果的json数据组
        BoolQueryBuilder bqb = QueryBuilders.boolQuery();//构建ES的bool查询
        for (String key : fieldMap.keySet()) {//根据查询的条件，构建bool查询的must描述
            TermQueryBuilder qb = QueryBuilders.termQuery(key, fieldMap.get(key));
            bqb.must(qb);
        }
        /**
         * The number of search hits to return. Defaults to <tt>10</tt>.
         */
        SearchResponse sr = client.prepareSearch(index).setTypes(type).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(bqb)
                .setFrom(from)
//                .setPreference("_only_nodes:node02")
                .setPreference("_shards:4")
                .setSize(pageSize).get();//构建查询
        SearchHits hits = sr.getHits();//执行查询，返回结果
        for (SearchHit hit : hits) {
            String json = hit.getSourceAsString();
            list.add(json);
        }
        return list;
    }

    /***
     * @Param
     * @[Java]Param [client, index, type, fieldMap]
     * @description 根据传入的页数及每页的数量查询分页数据
     * @author luyj
     * @date 2020/3/30 下午10:16
     * @return
     * @[java]return java.util.List<java.lang.String>
     */
    public synchronized List<String> searchAllPages(TransportClient client,
                                                    String index,
            String type,
            Map<String, String> fieldMap,
            Integer from,
            Integer pageSize) {
        Integer pages = 0;
        ArrayList<String> list = new ArrayList<>();//用于保存查询结果的json数据组
        BoolQueryBuilder bqb = QueryBuilders.boolQuery();//构建ES的bool查询
        for (String key : fieldMap.keySet()) {//根据查询的条件，构建bool查询的must描述
            TermQueryBuilder qb = QueryBuilders.termQuery(key, fieldMap.get(key));
            bqb.must(qb);
        }
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setIndices(index).setTypes(type).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(bqb).setSize(pageSize);
        searchRequestBuilder.setScroll(TimeValue.timeValueMinutes(1));//设置 search context 维护1分钟的有效期
        SearchResponse scrollResp = searchRequestBuilder.execute().actionGet();//获得首次的查询结果

        if (from == 1) {//如果查询的第一页，则将第一页结果返回
            for (SearchHit searchHit : scrollResp.getHits().getHits()) {

                String json = searchHit.getSourceAsString();
                list.add(json);

            }
        }
        pages += 1;//查询的页数增加1
        System.out.println(pages);

        while (from - pages > 0 && scrollResp.getHits().getHits().length != 0) {//继续查询直到查询到目标页
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(TimeValue.timeValueMinutes(1))
                    .execute().actionGet();
//            client.clearScroll()
            pages += 1;
            System.out.println(pages);
            if (from == pages) {//如果查询到目标页则取出
                for (SearchHit searchHit : scrollResp.getHits().getHits()) {
                    String json = searchHit.getSourceAsString();
                    list.add(json);
                }
            }

        }
        ;
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollResp.getScrollId());
        client.clearScroll(clearScrollRequest).actionGet();

        return list;

    }

    public synchronized List<String> searchTerm(TransportClient client, String index, String type, String fieldKey, String fieldValue) {

        List<String> list = new ArrayList<>();

        if (fieldValue != null && !"".equals(fieldValue)) {

            QueryBuilder qb = QueryBuilders.termQuery(fieldKey, fieldValue);

            SearchResponse sr = client.prepareSearch(index).setTypes(type).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(qb).get();

            SearchHits hits = sr.getHits();

            for (SearchHit hit : hits) {
                String json = hit.getSourceAsString();

                list.add(json);
            }

        }

        return list;
    }

    public synchronized Map<Object, String> searchTermsIDAndJson(TransportClient client, String index, String type, int from, int size, Map<String, String> fieldMap) {

        Map<Object, String> map = new HashMap<>();
        // 来获取查询数据信息

        BoolQueryBuilder bqb = QueryBuilders.boolQuery();
        for (String key : fieldMap.keySet()) {

            QueryBuilder qb = QueryBuilders.termQuery(key, fieldMap.get(key));
            bqb.must(qb);

        }

        SearchResponse sr = client.prepareSearch(index).setTypes(type).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(bqb).setFrom(from).setSize(size).get();

        SearchHits hits = sr.getHits();

        for (SearchHit hit : hits) {
            String json = hit.getSourceAsString();
            map.put(hit.getId(), json);
        }

        return map;
    }

    public synchronized List<String> searchTerms( TransportClient client, String index, String type, int from, int size, Map<String, String> fieldMap) {

        List<String> list = new ArrayList<String>();
        // 来获取查询数据信息

        BoolQueryBuilder bqb = QueryBuilders.boolQuery();
        for (String key : fieldMap.keySet()) {

            QueryBuilder qb = QueryBuilders.termQuery(key, fieldMap.get(key));
            bqb.must(qb);

        }

        SearchResponse sr = client.prepareSearch(index).setTypes(type).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(bqb).setFrom(from).setSize(size).get();

        SearchHits hits = sr.getHits();

        for (SearchHit hit : hits) {
            hit.getId();
            String json = hit.getSourceAsString();
            list.add(JSON.toJSONString(json));
        }

        return list;
    }

    public synchronized String searchId( TransportClient client, String index, String type, String id) {

        if (id != null && !"".equals(id)) {
            GetRequestBuilder getRequestBuilder = client.prepareGet(index, type, id);
            GetResponse getResponse = getRequestBuilder.execute().actionGet();
            return getResponse.getSourceAsString();
        }

        return "";
    }

    public synchronized GetResponse searchIdAsResponse( TransportClient client, String index, String type, String id) {

        if (id != null && !"".equals(id)) {
            GetRequestBuilder getRequestBuilder = client.prepareGet(index, type, id);
            GetResponse getResponse = getRequestBuilder.execute().actionGet();
            return getResponse;
        }
        return null;
    }

    public synchronized boolean insertES( TransportClient client, String index, String type, String id, String json) {


        if (id != null && !"".equals(id)) {

            IndexResponse ir = client.prepareIndex(index, type, id).setSource(json, XContentType.JSON).get();

            // 这里也有指定的时间获取返回值的信息，如有特殊需求可以
            int n = ir.getShardInfo().getSuccessful();
            if (n >= 1) {
                return true;
            }
        }

        return false;
    }
    /*
    自动生成ID写入ES的方法
     */

    public synchronized boolean insertESNoID(TransportClient client, String index, String type, String json) {


        IndexResponse ir = client.prepareIndex(index, type).setSource(json, XContentType.JSON).get();

        // 这里也有指定的时间获取返回值的信息，如有特殊需求可以
        int n = ir.getShardInfo().getSuccessful();
        if (n >= 1) {
            return true;
        }


        return false;
    }


    public static void batchInsertEs(TransportClient client, Map<String, String> map, String index, String type) {

        BulkRequestBuilder bulk = client.prepareBulk();

        int count = 0;

        for (String id : map.keySet()) {

            bulk.add(client.prepareIndex(index, type, id).setSource(map.get(id), XContentType.JSON));

            if (count == 300) {
                count = 0;

                bulk.execute().actionGet();
            }
        }

        bulk.execute().actionGet();
    }

    public void updateES( TransportClient client, String index, String type, String id, JSONObject data) {

        if (index == null || type == null || id == null) {
            return;
        }

        //更新步骤

        UpdateRequest up = new UpdateRequest();

        up.index(index).type(type).id(id).doc(data);

        //获取响应信息
        //.actionGet(timeoutMillis)，也可以用这个方法，当过了一定的时间还没得到返回值的时候，就自动返回。
        UpdateResponse response = client.update(up).actionGet();
    }

    /**
     * 根据属性值查询删除
     */
    public void deleteByQuery( TransportClient client, String index, String type, String key, String value) {
        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.typeQuery(type))
                .filter(QueryBuilders.termQuery(key, value))
                .source(index)
                .get();
        long deleted = response.getDeleted();
        System.out.println(deleted + "item removed in ES");
    }

    /**
     * 根据属性值查询删除
     */
    public void updateByQuery(TransportClient client, String index, String key, String value) {
        BulkByScrollResponse response = UpdateByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery(key, value))
                .source(index)
                .get();
        long deleted = response.getDeleted();
        System.out.println(deleted);
    }

    public static void batchUpdateEs(TransportClient client, String index, Map<String, String> queryMap, String updateKey, String updateValue) {

        //使用ElasticSearch的script来存放脚本 ， 让userName修改为newName
        StringBuffer sb = new StringBuffer();
        sb.append("ctx._source.").append(updateKey).append("=").append(updateValue);
        Script script = new Script(sb.toString());
        //创建更新的条件，假如是orgPath要能满足orgInfo[0]这个条件的，就修改userName
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (String key : queryMap.keySet()) {
            boolQuery.must(QueryBuilders.termQuery(key, queryMap.get(key)));
        }
        //执行
        UpdateByQueryAction.INSTANCE.newRequestBuilder(client).source(index).script(script).filter(boolQuery).get();

    }

    public static void batchInsertEs(TransportClient client, List<String> list) {

        BulkRequestBuilder bulk = client.prepareBulk();

        int count = 0;

        for (String json : list) {

            bulk.add(client.prepareIndex("db_msg", "info").setSource(json, XContentType.JSON));

            if (count == 3000) {
                count = 0;

                bulk.execute().actionGet();
            }
        }

        bulk.execute().actionGet();
    }

    public synchronized String searchRoutingId(TransportClient client, String index, String type, String id,String routing) {

        if (id != null && !"".equals(id)) {
            GetRequestBuilder getRequestBuilder = client.prepareGet(index, type, id).setRouting(routing);
            GetResponse getResponse = getRequestBuilder.execute().actionGet();
            return getResponse.getSourceAsString();
        }

        return "";
    }
}
