package com.elasticplayground.client;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by luosai on 2016/12/16.
 */
public class Demo {// on startup

     private  static  String index = "twitter" ;
     private  static String type = "twit" ;
    public static TransportClient getClient() throws UnknownHostException {
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        return  client ;
    }
    public static void main(String[] args) {
        try {
            TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

//            Map<String, Object> json = new HashMap<String, Object>();
//            json.put("user","kimchy");
//            json.put("postDate",new Date());
//            json.put("message","trying out Elasticsearch");
//
//            IndexResponse response = client.prepareIndex("twitter", "tweet")
//                    .setSource(json)
//                    .get();
//
//            String id = response.getId();
//            String index = response.getIndex();
//            long version = response.getVersion();
//            ShardId shardId = response.getShardId();
//            System.out.println("id = "+id +" index = "+index + " version = "+version);

//            deleteAPI();
//             addAPI();
//            getAPI();
//            mutilGetAPI();
//            updateAPI();
//            bulkAPI();
            matchQuery();
//            mutilGetAPI();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }



    public static void addAPI() throws IOException {

        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
         XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field("user", "zhangsan")
                .field("postDate", new Date())
                .field("message", "i love you").endObject();
        String jsonData = xContentBuilder.string();
        System.out.println(jsonData);
        IndexResponse indexResponse = client.prepareIndex(index,type,"3").setSource(jsonData).get();
        DocWriteResponse.Result result = indexResponse.getResult();
        System.out.println(result.toString()+" id = "+indexResponse.getId()+"sharedId = " +indexResponse.getShardId().getId());
    }

    public static void  updateAPI() throws IOException {
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(index);
        updateRequest.type(type);
        updateRequest.id("2") ;

        updateRequest.doc(  XContentFactory.jsonBuilder().startObject()
                .field("message", "i missed you").endObject());
        client.update(updateRequest) ;

    }

   public static void deleteAPI() throws UnknownHostException {

       TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
               .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
       DeleteResponse deleteResponse = client.prepareDelete(index, type, "1").get();
       int status = deleteResponse.status().getStatus();
       String id = deleteResponse.getId();
       ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
       ShardId shardId = deleteResponse.getShardId();
       System.out.println("status = " +status +" id = " +id +" shareId = " + shardId.getId());
   }

    public static void  getAPI() throws UnknownHostException {

        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        GetResponse getRequestBuilder = client.prepareGet(index, type, "2").get();

        String sourceAsString = getRequestBuilder.getSourceAsString();
        System.out.println("getAPI one recode: "+sourceAsString);



    }

    public static  void  mutilGetAPI() throws UnknownHostException {
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet().add(index, type, "2", "3","4","5").get();
        for (MultiGetItemResponse multiGetItemResponse:multiGetItemResponses){
            String sourceAsString = multiGetItemResponse.getResponse().getSourceAsString();
            System.out.println("json : "+sourceAsString);
        }
    }
// on shutdown

    //批量处理多个
    public  static void bulkAPI() throws IOException{
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

    BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        bulkRequestBuilder.add(client.prepareIndex(index,type,"4").setSource(XContentFactory.jsonBuilder()
        .startObject().field("user","lisisi").field("postDate",new Date()).field("message","i am hungry").field("age",24).endObject()));
        Map<String,Object> map = new HashMap<String, Object>();
        map.put("id",1);
        map.put("postDate",new Date());
        map.put("message","you know me i love you ");
        map.put("sex",1);
        map.put("age",16);
        map.put("user","zhang");
        bulkRequestBuilder.add(client.prepareIndex(index,type,"5").setSource( map));

        BulkResponse bulkItemResponses = bulkRequestBuilder.get();
        if (bulkItemResponses.hasFailures()){
            for (BulkItemResponse bulkResponse:bulkItemResponses){
                RestStatus status = bulkResponse.status();
                System.out.println(status);
            }
            String s = bulkItemResponses.buildFailureMessage();
            System.out.println("failure message "+s);
        }

    }

    public static void matchQuery()throws IOException{
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        //match query 精确匹配 模糊匹配没查出来
        QueryBuilder matchQuery ;
         matchQuery = QueryBuilders.matchQuery("user", "zhang");
        //多个字段匹配 text,fields ;
         matchQuery = QueryBuilders.multiMatchQuery("zhangsan","user","massage");
        // 常用词查询
         matchQuery =  QueryBuilders.commonTermsQuery("user","zhangsan");
        //结构化数据查询 精确匹配

         matchQuery = QueryBuilders.termQuery("user", "zhangsan");
        //  rang 范围查询
         matchQuery = QueryBuilders.rangeQuery("age").from("5").to("20").includeLower(true).includeUpper(true);
        //前缀查询
         matchQuery = QueryBuilders.prefixQuery("user","li");
        //模糊查询
         matchQuery = QueryBuilders.wildcardQuery("user","zh?ng*");
        //type 查询
         matchQuery = QueryBuilders.typeQuery(type) ;
        //ids 查询
         matchQuery = QueryBuilders.idsQuery(type).addIds("2","3","4","5");
        //常量分数查询
         matchQuery = QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("user","zhangsan")).boost(2.0f);
        //条件查询
        matchQuery = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("user","zhangsan"))
                .must(QueryBuilders.termQuery("message","i"))
                .mustNot(QueryBuilders.termQuery("message","love"));
        SearchResponse searchResponse = client.prepareSearch(index).setTypes(type).setQuery(matchQuery).get();
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit searchHit:hits){
            String sourceAsString = searchHit.getSourceAsString();
            System.out.println(sourceAsString);
        }
    }
}
