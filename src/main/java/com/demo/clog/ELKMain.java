package com.demo.clog;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import net.sf.json.JSON;
import net.sf.json.JSONObject;
import net.sf.json.JSONString;
import org.apache.commons.lang.UnhandledException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.w3c.dom.CDATASection;
import sun.plugin2.message.Message;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ELKMain {

    public static String clusterName = "cluster.name";
    public static String appName = "my-application";
    public static String inetAddr = "192.168.126.122";
    public static int clientPort = 9300;
    public static String elkIndex = "logstash-nginx-access-log";
    public static String elkType = "doc";
    public static String elkId1 = "ANBCkmgBM03z9jfj8nu3";
    public static Integer response = 200;


    //获取ELK客户端
    public static TransportClient getClient() throws UnknownHostException {
        //指定ES集群
        Settings settings = Settings.builder().put(clusterName, appName).build();
        //创建访问ES的客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName(inetAddr), clientPort));
        return client;
    }

    //通过ID查询
    public static void doQuery1() throws UnknownHostException {
        TransportClient client = getClient();
        GetResponse response = client.prepareGet(elkIndex, elkType, elkId1).execute().actionGet();
        Map<String, Object> map = response.getSourceAsMap();
        System.out.println("rsp: " + response.getSourceAsString());
        System.out.println("map: " + map.get("agent"));
    }

    //查询所有
    public static void doQuery2() throws UnknownHostException {
        TransportClient client = getClient();
        QueryBuilder qb = QueryBuilders.matchAllQuery();
        SearchResponse sr = client.prepareSearch(elkIndex).setQuery(qb).get();
        SearchHits hits = sr.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //match查询
    public static void doQuery3() throws UnknownHostException {
        TransportClient client = getClient();
        QueryBuilder qb = QueryBuilders.matchAllQuery();
        //Time.timeValueMinutes-----这个设置查询超时时间
        SearchResponse sr = client.prepareSearch(elkIndex).setQuery(qb).setScroll(TimeValue.timeValueMinutes(2)).get();
        SearchHits hits = sr.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //multiMatch查询
    public static void doQuery4() throws UnknownHostException {
        TransportClient client = getClient();
        QueryBuilder qb = QueryBuilders.multiMatchQuery("x64", "Mozilla/5.0", "Win64;", "agent");
        SearchResponse sr = client.prepareSearch(elkIndex).setQuery(qb).setSize(10).get();
        SearchHits hits = sr.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //term查询
    public static void doQuery5() throws UnknownHostException {
        TransportClient client = getClient();
        QueryBuilder qb = QueryBuilders.termQuery("response", "aaa");
        SearchResponse sr = client.prepareSearch(elkIndex).setSize(10).get();
        SearchHits hits = sr.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //terms查询
    public static void doQuery6() throws UnknownHostException {
        TransportClient client = getClient();
        QueryBuilder qb = QueryBuilders.termsQuery("response", "xxx", "200");
        SearchResponse sr = client.prepareSearch(elkIndex).setQuery(qb).setSize(3).get();
        SearchHits hits = sr.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //range查询
    public static void doQuery7() throws UnknownHostException {


        TransportClient client = getClient();
        RangeQueryBuilder qb1 = QueryBuilders.rangeQuery("create_time").from("2019-02-22T06:11:55.778Z").to("2019-02-26T23:11:55.236Z");
        SearchResponse sr = client.prepareSearch(elkIndex).setQuery(qb1).execute().actionGet();
        SearchHits hits = sr.getHits();
        System.out.println(hits.getHits().length);
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsMap().get("message"));
        }

    }

    //prefix查询
    public static void doQuery8() throws UnknownHostException {
        TransportClient client = getClient();
        QueryBuilder qb = QueryBuilders.prefixQuery("response", "404");
        SearchResponse sr = client.prepareSearch(elkIndex).setQuery(qb).setSize(10).get();
        SearchHits hits = sr.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //wildcard查询
    public static void doQuery9() throws UnknownHostException {
        TransportClient client = getClient();
        QueryBuilder qb = QueryBuilders.wildcardQuery("http_version", "1.*");
        SearchResponse sr = client.prepareSearch(elkIndex).setQuery(qb).setSize(3).get();
        SearchHits hits = sr.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //fuzzy查询
    public static void doQuery10() throws UnknownHostException {
        TransportClient client = getClient();
        QueryBuilder qb = QueryBuilders.fuzzyQuery("agent", "ozilla");
        SearchResponse sr = client.prepareSearch(elkIndex).setQuery(qb).setSize(3).get();
        SearchHits hits = sr.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //聚合查询，查询最大值
    public static void doQuery11() throws UnknownHostException {
        TransportClient client = getClient();
        AggregationBuilder agg = AggregationBuilders.max("aggMax").field("offset");
        SearchResponse sr = client.prepareSearch(elkIndex).addAggregation(agg).get();
        Max max = sr.getAggregations().get("aggMax");
        System.out.println(max.getValue());
    }

    //聚合查询，查询最小值
    public static void doQuery12() throws UnknownHostException {
        TransportClient client = getClient();
        AggregationBuilder agg = AggregationBuilders.min("aggMin").field("offset");
        SearchResponse sr = client.prepareSearch(elkIndex).addAggregation(agg).get();
        Min min = sr.getAggregations().get("aggMin");
        System.out.println(min.getValue());
    }

    //聚合查询，查询平均值
    public static void doQuery13() throws UnknownHostException {
        TransportClient client = getClient();
        AggregationBuilder agg = AggregationBuilders.avg("aggAvg").field("offset");
        SearchResponse sr = client.prepareSearch(elkIndex).addAggregation(agg).get();
        Avg avg = sr.getAggregations().get("aggAvg");
        System.out.println(avg.getValue());
    }

    //聚合查询，计算总和
    public static void doQuery14() throws UnknownHostException {
        TransportClient client = getClient();
        AggregationBuilder agg = AggregationBuilders.sum("Sum").field("offset");
        SearchResponse sr = client.prepareSearch(elkIndex).addAggregation(agg).get();
        Sum sum = sr.getAggregations().get("Sum");
        System.out.println(sum.getValue());
    }

    //聚合查询，基数查询
    public static void doQuery15() throws UnknownHostException {
        TransportClient client = getClient();
        AggregationBuilder agg = AggregationBuilders.cardinality("aggCardinalityXX").field("offset");
        SearchResponse sr = client.prepareSearch(elkIndex).addAggregation(agg).get();
        Cardinality cardinality = sr.getAggregations().get("aggCardinalityXX");
        System.out.println(cardinality.getValue());
    }

    //聚合查询
    public static void doQuery16() throws UnknownHostException {
        TransportClient client = getClient();
        QueryBuilder qb = QueryBuilders.commonTermsQuery("response", "404");
        SearchResponse sr = client.prepareSearch(elkIndex).setQuery(qb).get();
        SearchHits hits = sr.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //聚合查询
    public static void doQuery17() throws UnknownHostException {
        TransportClient client = getClient();
        QueryBuilder qb = QueryBuilders.queryStringQuery("+changge -hejiu");
        SearchResponse sr = client.prepareSearch(elkIndex).setQuery(qb).get();
        SearchHits hits = sr.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //组合查询
    public static void doQuery18() throws UnknownHostException {
        TransportClient client = getClient();
        /*QueryBuilder qb = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("response", "304"))
                .mustNot(QueryBuilders.matchQuery("http_version", "2.2"))
                .should(QueryBuilders.matchQuery("agent", "MMM"))
                .filter(QueryBuilders.rangeQuery("offset").gte(814));*/
        //constantscore
        QueryBuilder qb = QueryBuilders.constantScoreQuery(QueryBuilders.matchQuery("response", "304"));
        SearchResponse sr = client.prepareSearch(elkIndex).setQuery(qb).get();
        SearchHits hits = sr.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //分组聚合
    public static void doQuery19() throws UnknownHostException {
        TransportClient client = getClient();
        AggregationBuilder agg = AggregationBuilders.terms("terms").field("offset");
        SearchResponse sr = client.prepareSearch(elkIndex).addAggregation(agg).execute().actionGet();
        Terms terms = sr.getAggregations().get("terms");
        for (Terms.Bucket entry : terms.getBuckets()) {
            System.out.println(entry.getKey() + ":" + entry.getDocCount());
        }
    }

    //过滤聚合
    public static void doQuery20() throws UnknownHostException {
        TransportClient client = getClient();
        QueryBuilder qb = QueryBuilders.termQuery("offset", 2172);
        AggregationBuilder agg = AggregationBuilders.filter("filter", qb);
        SearchResponse sr = client.prepareSearch(elkIndex).addAggregation(agg).execute().actionGet();
        Filter filter = sr.getAggregations().get("filter");
        System.out.println(filter.getDocCount());
    }

    //多条件过滤
    public static void doQuery21() throws UnknownHostException {
        TransportClient client = getClient();
        AggregationBuilder agg = AggregationBuilders.filters("filters",
                new FiltersAggregator.KeyedFilter("changge", QueryBuilders.termQuery("interests", "changge")),
                new FiltersAggregator.KeyedFilter("hejiu", QueryBuilders.termQuery("interests", "hejiu")));
        SearchResponse sr = client.prepareSearch(elkIndex).addAggregation(agg).execute().actionGet();
        Filters filters = sr.getAggregations().get("filters");
        for (Filters.Bucket entry : filters.getBuckets()) {
            System.out.println(entry.getKey() + ":" + entry.getDocCount());
        }
    }

    //范围过滤
    public static void doQuery22() throws UnknownHostException {
        TransportClient client = getClient();
        AggregationBuilder agg = AggregationBuilders
                .range("range")
                .field("offset")
                .addUnboundedTo(2500)
                .addRange(500, 2500)
                .addUnboundedFrom(500);
        SearchResponse sr = client.prepareSearch(elkIndex).addAggregation(agg).execute().actionGet();
        Range range = sr.getAggregations().get("range");
        for (Range.Bucket entry : range.getBuckets()) {
            System.out.println(entry.getKey() + ":" + entry.getDocCount());
        }
    }

    //missing聚合，判断值为null的
    public static void doQuery23() throws UnknownHostException {
        TransportClient client = getClient();
        AggregationBuilder agg = AggregationBuilders.missing("missing").field("offset");
        SearchResponse sr = client.prepareSearch(elkIndex).addAggregation(agg).execute().actionGet();
        Aggregation aggregation = sr.getAggregations().get("missing");
        System.out.println(aggregation.toString());
    }

    //查询ES中所有的索引
    public static void getAllIndices() throws UnknownHostException {
        TransportClient client = getClient();
        ActionFuture<IndicesStatsResponse> isr = client.admin().indices().stats(new IndicesStatsRequest().all());
        Set<String> set = isr.actionGet().getIndices().keySet();
        System.out.println(set);
    }

    //获取索引库中所有的索引字段和字段类型
    public static Set<String> getAllIndices(String fieldName, Map<String, Object> mapProperties) {
        Set<String> fieldSet = new HashSet<String>();
        Map<String, Object> map = (Map<String, Object>) mapProperties.get("properties");

        Set<String> keys = map.keySet();
        for (String key : keys) {
            if (((Map<String, Object>) map.get(key)).containsKey("type")) {
                fieldSet.add(fieldName + "" + key);
            } else {
                Set<String> tempList = getAllIndices(fieldName + "" + key
                        + ".", (Map<String, Object>) map.get(key));
                fieldSet.addAll(tempList);
            }
        }
        return fieldSet;
    }
    public static void getAllFields() throws UnknownHostException {

        TransportClient client = getClient();
        Set fieldSet = new HashSet<String>();
        ClusterState cs = client.admin().cluster().prepareState()
                .setIndices(elkIndex).execute().actionGet().getState();
        IndexMetaData imd = cs.getMetaData().index(elkIndex);
        MappingMetaData mdd = imd.mapping(elkType);

        //System.out.println(mdd);

        Map<String, Object> mapProperties = new HashMap<String, Object>();

        try {
            mapProperties = mdd.getSourceAsMap();
        } catch (Exception e) {
            e.printStackTrace();
        }
        fieldSet = (Set) getAllIndices("", mapProperties);
        System.out.println("Field List:");
        for (Object field : fieldSet) {
            System.out.println(field);
        }
    }

    //对所有字段分词查询
    public static void query() throws UnknownHostException {
        TransportClient client = getClient();
        // 1 条件查询
        SearchResponse searchResponse = client.prepareSearch("logstash-nginx-access-log").setTypes("doc")
                .setQuery(QueryBuilders.queryStringQuery("全文")).get();

        // 2 打印查询结果
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");

        Iterator<SearchHit> iterator = hits.iterator();

        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next(); // 每个查询对象

            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
        }

    }



    /*
     * 建立索引,索引建立好之后,会在elasticsearch-0.20.6\data\elasticsearch\nodes\0创建所以你看
     * @param indexName  为索引库名，一个es集群中可以有多个索引库。 名称必须为小写
     * @param indexType  Type为索引类型，是用来区分同索引库下不同类型的数据的，一个索引库下可以有多个索引类型。
     * @param jsondata     json格式的数据集合
     *
     * @return
     */
    public static void createIndexResponse(String indexname, String type, List<String> jsondata) throws UnknownHostException {
        TransportClient client = getClient();
        //创建索引库 需要注意的是.setRefresh(true)这里一定要设置,否则第一次建立索引查找不到数据
        IndexRequestBuilder requestBuilder = client.prepareIndex(indexname, type);

        for (int i = 0; i < jsondata.size(); i++) {
            requestBuilder.setSource(jsondata.get(i)).execute().actionGet();
        }
    }


    //prefix查询（前缀查询）
    public static void queryElkLogType() throws UnknownHostException {
        TransportClient client = getClient();
        QueryBuilder qb = QueryBuilders.prefixQuery("response", "4*,5*");
        SearchResponse sr = client.prepareSearch(elkIndex).setQuery(qb).get();
        SearchHits hits = sr.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //按指定字段进行降序排序，返回message信息
    public static void queryRealTime() throws UnknownHostException {
        TransportClient client = getClient();
        QueryBuilder qb = QueryBuilders.matchAllQuery();
        SearchResponse sr = client.prepareSearch(elkIndex).setQuery(qb).addSort("create_time", SortOrder.DESC).setSize(1).execute().actionGet();
        SearchHits hits = sr.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsMap().get("message").toString());
        }
    }

    //按照指定字段进行聚合统计
    public static void selectCount() throws UnknownHostException{
        TransportClient client = getClient();
        //QueryBuilder qb = QueryBuilders.rangeQuery("create_time").from().toString();
        AggregationBuilder agg = AggregationBuilders.stats("set").field("offset");
        SearchResponse sr = client.prepareSearch(elkIndex).
                            addAggregation(agg).
                            execute().actionGet();
        Stats stats = sr.getAggregations().get("set");
        System.out.println(stats.getCount());
    }

    //按请求时间段进行聚合统计(0-1)(1-2)(2-3)(3-4)...
    public static void timeGroupBy() throws UnknownHostException{
        String indexType = "doc";
        //创建连接
        TransportClient client = getClient();
        //按时间进行范围查询
        QueryBuilder qb1 = QueryBuilders.rangeQuery("create_time").from("2019-02-25T06:11:55.778Z").to("2019-02-28T23:11:55.236Z");
        QueryBuilder mutilQuery = QueryBuilders.multiMatchQuery("response", "4*","5*");
        //按异常进行分组
        AggregationBuilder termsBuilder = AggregationBuilders.terms("by_response").field("response");
        SearchResponse searchResponse = client.prepareSearch(elkIndex).
                setTypes(indexType).
                setQuery(qb1).
                setQuery(mutilQuery).
                addAggregation(termsBuilder).
                execute().actionGet();
        Terms terms = searchResponse.getAggregations().get("by_response");
        //循环遍历bucket桶
        for (Terms.Bucket entry: terms.getBuckets() ){
            System.out.println(entry.getKey()+":"+entry.getDocCount());
        }
    }

    //根据索引名称查询所有索引的字段和类型
    public static void selectFile() throws Exception{
        ImmutableOpenMap<String,MappingMetaData> mappings = null;
        String mapping = "";
        try {
            TransportClient client = getClient();

            mappings = client.admin().cluster()
                                            .prepareState().execute().actionGet().getState()
                                            .getMetaData().getIndices().get(elkIndex)
                                            .getMappings();

            mapping = mappings.get(elkType).source().toString();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        JSONObject jsonObject = JSONObject.fromObject(mapping);

        String doc = jsonObject.getString("doc");

        JSONObject jsonObject1 = JSONObject.fromObject(doc);

        String properties = jsonObject1.getString("properties");

        JSONObject jsonObject2 = JSONObject.fromObject(properties);

        Map<String,Map<String,String>> map = jsonObject2;

        for (Map.Entry<String,Map<String,String>> str :map.entrySet()){
            String key = str.getKey();
            for (Map.Entry<String,String> ms :str.getValue().entrySet()){
                if (ms.getKey().equals("type")){
                    System.out.println(key+":"+ms.getValue());
                }

            }

        }
    }




    //测试
    public static void main(String[] args) throws Exception {

        //doQuery1();
        //doQuery2();
        //doQuery3();
        //doQuery4();
        //doQuery5();
        //doQuery6();
        //doQuery7();
        //doQuery8();
        //doQuery9();
        //doQuery10();
        //doQuery11();
        //doQuery12();
        //doQuery13();
        //doQuery14();
        //doQuery15();
        //doQuery16();
        //doQuery17();
        //doQuery18();
        //doQuery19();
        //doQuery20();
        //doQuery21();
        //doQuery22();
        //doQuery23();
        //getAllIndices();
        //getAllFields();
        //getIndexMappings("");
        //query();
        //queryResponse;
        //?createIndexResponse("1","nginx","");
        //queryElkLogType();
        //timeGroupBy
        selectFile();

    }
}
