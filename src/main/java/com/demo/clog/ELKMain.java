package com.demo.clog;

import javafx.beans.property.MapProperty;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.PrivateKey;
import java.util.*;

public class ELKMain {

    public static String clusterName = "cluster.name";
    public static String appName = "my-application";
    public static String inetAddr = "192.168.126.122";
    public static int clientPort = 9300;
    public static String elkIndex = "logstash-nginx-access-log";
    public static String elkType = "doc";
    public static String elkId1 = "6L-zeWgB94PVfPphkTPm";

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
        SearchResponse sr = client.prepareSearch(elkIndex).setQuery(qb).setSize(3).get();
        SearchHits hits = sr.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //match查询
    public static void doQuery3() throws UnknownHostException {
        TransportClient client = getClient();
        QueryBuilder qb = QueryBuilders.matchQuery("agent", "Mozilla/5.0");
        SearchResponse sr = client.prepareSearch(elkIndex).setQuery(qb).setSize(3).get();
        SearchHits hits = sr.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //multiMatch查询
    public static void doQuery4() throws UnknownHostException {
        TransportClient client = getClient();
        QueryBuilder qb = QueryBuilders.multiMatchQuery("x64", "Mozilla/5.0", "Win64;", "agent");
        SearchResponse sr = client.prepareSearch(elkIndex).setQuery(qb).setSize(3).get();
        SearchHits hits = sr.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //term查询
    public static void doQuery5() throws UnknownHostException {
        TransportClient client = getClient();
        QueryBuilder qb = QueryBuilders.termQuery("response", "404");
        SearchResponse sr = client.prepareSearch(elkIndex).setQuery(qb).setSize(3).get();
        SearchHits hits = sr.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //terms查询
    public static void doQuery6() throws UnknownHostException {
        TransportClient client = getClient();
        QueryBuilder qb = QueryBuilders.termsQuery("response", "xxx", "304");
        SearchResponse sr = client.prepareSearch(elkIndex).setQuery(qb).setSize(3).get();
        SearchHits hits = sr.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //range查询
    public static void doQuery7() throws UnknownHostException {
        TransportClient client = getClient();
        QueryBuilder qb = QueryBuilders.rangeQuery("offset").from(3).to(9999);
        SearchResponse sr = client.prepareSearch(elkIndex).setQuery(qb).setSize(3).get();
        SearchHits hits = sr.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //prefix查询
    public static void doQuery8() throws UnknownHostException {
        TransportClient client = getClient();
        QueryBuilder qb = QueryBuilders.prefixQuery("response", "304");
        SearchResponse sr = client.prepareSearch(elkIndex).setQuery(qb).setSize(3).get();
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
        AggregationBuilder agg = AggregationBuilders.sum("aggSum").field("offset");
        SearchResponse sr = client.prepareSearch(elkIndex).addAggregation(agg).get();
        Sum sum = sr.getAggregations().get("aggSum");
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
    public static void getAllIndices() throws UnknownHostException{
        TransportClient client = getClient();
        ActionFuture<IndicesStatsResponse> isr = client.admin().indices().stats(new IndicesStatsRequest().all());
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        Map<String, IndexStats> indexStatsMap = isr.actionGet().getIndices();
        Set<String> set = isr.actionGet().getIndices().keySet();
        System.out.println(set);
    }

        //获取索引库中所有的索引字段
       public static List<String> getIndexFieldList(String fieldName, Map<String, Object> mapProperties) {
            List<String> fieldList = new ArrayList<String>();
            Map<String, Object> map = (Map<String, Object>) mapProperties.get("properties");

           Set<String> keys = map.keySet();
            for (String key : keys) {
                if (((Map<String, Object>) map.get(key)).containsKey("type")) {
                    fieldList.add(fieldName + "" + key);
                } else {
                    List<String> tempList = getIndexFieldList(fieldName + "" + key
                            + ".", (Map<String, Object>) map.get(key));
                    fieldList.addAll(tempList);
                }
            }
            return fieldList;
        }
        public static void getAllFields() throws UnknownHostException{

            TransportClient client = getClient();
            List<String> fieldList = new ArrayList<String>();
            ClusterState cs = client.admin().cluster().prepareState()
                    .setIndices(elkIndex).execute().actionGet().getState();
            IndexMetaData imd = cs.getMetaData().index(elkIndex);
            MappingMetaData mdd = imd.mapping(elkType);

            System.out.println("111111"+mdd.getSourceAsMap());

            Map<String, Object> mapProperties = new HashMap<String, Object>();

            try {
                mapProperties = mdd.getSourceAsMap();
            } catch (Exception e) {
                e.printStackTrace();
            }
            fieldList =getIndexFieldList("", mapProperties);
            System.out.println("Field List:");
            for (Object field : fieldList) {
                System.out.println(field);
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
        getAllFields();

    }
}
