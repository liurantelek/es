package com.liuran.es.esapidemo.demo1.serviceImpl;

import com.liuran.es.esapidemo.demo1.init.InitEs;
import com.liuran.es.esapidemo.demo1.service.HighEsService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.Inet4Address;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @version v1.0
 * @ProjectName: es-api-demo
 * @ClassName: HighEsServiceImpl
 * @Description: TODO(一句话描述该类的功能)
 * @Author: 刘然
 * @Date: 2020/9/15 18:25
 */
@Service
public class HighEsServiceImpl implements HighEsService {

    @Override
    public String indexDocuments(String indexName, String document) {
        buildIndexRequestWithString(indexName,document,"kkk");
        return null;
    }

    @Override
    public String getRequest(String indexName, String type,String id) {
        GetRequest getRequest = new GetRequest(indexName,type,id);
        //禁止源检索，在默认情况下启用
        getRequest.fetchSourceContext(FetchSourceContext.FETCH_SOURCE);
        //w为特定字段配置源包含
        String[] inccludes = new String[]{"name","value"};
        //为特定字段配置排除源
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true,
                inccludes,excludes);
        getRequest.fetchSourceContext(fetchSourceContext);
        //为特定存储字段配置检索
        getRequest.storedFields("name");
        FetchSourceContext fetchContext = getRequest.fetchSourceContext();
        getRequest.storedFields("name");
        GetResponse response = null;
        try {
            response = InitEs.restClient.get(getRequest, RequestOptions.DEFAULT);
            Map<String, DocumentField> fields = response.getFields();
            Map<String, Object> source = response.getSource();
            String name = (String) source.get("name");
            System.out.println(name);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public void executeBulkRequest(String indexName, String field) {

        BulkRequest bulkRequest = new BulkRequest();
        //添加第一个indexRequest
        IndexRequest request = new IndexRequest("indexname");
        Map<String,Object> source = new HashMap<>();
        source.put("1","1");
        source.put("2","2");
        request.id("1").source(source);

        IndexRequest request2 = new IndexRequest("indexname2");
        Map<String,Object> source2 = new HashMap<>();
        source2.put("1","1");
        source2.put("2","2");
        request2.id("1").source(source2);
        bulkRequest.add(request).add(request2);
        try {
            BulkResponse itemResponses = InitEs.restClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            processBulkResponse(itemResponses);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void bulkProcess(String indexName, String field) {
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            //批量处理前的动作
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
                int numberOfActions = bulkRequest.numberOfActions();
                System.out.println("executionId"+l+",numberOfActions"+numberOfActions);
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                //批量处理后的动作
                if(bulkResponse.hasFailures()){
                    System.out.println("buld"+l+","+bulkResponse.getTook().getMillis()+"millisecondes");
                }
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                //批量处理后的动作
                System.err.println("falide to execuet bulk "+throwable.getMessage());
            }
        };

        BulkProcessor bulkProcessor = BulkProcessor.builder((bulkRequest, bulkResponseActionListener) -> InitEs.restClient.bulkAsync(
                bulkRequest,RequestOptions.DEFAULT,bulkResponseActionListener
        ),listener).build();
        //添加索引请求
        Map<String,Object> row = new HashMap<>();
        row.put("1","2");
        IndexRequest one = new IndexRequest(indexName).id("1").source(row);
        bulkProcessor.add(one);
        ;
    }

    /**
     *
     * @param fromIndex
     * @param toIndex
     */
    @Override
    public void executeReindex(String fromIndex, String toIndex) {
        ReindexRequest reindexRequest = new ReindexRequest();
        //添加源索引
        reindexRequest.setSourceIndices(fromIndex);
        //添加目标索引
        reindexRequest.setDestIndex(toIndex);
        try {
            BulkByScrollResponse bulkByScrollResponse = InitEs.restClient.reindex(reindexRequest,RequestOptions.DEFAULT);
            //解析response
            processBulkScrollResponse(bulkByScrollResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            InitEs.closeEs();
        }

    }

    //解析bulkByScrollResponse
    private void processBulkScrollResponse(BulkByScrollResponse bulkByScrollResponse) {
        if(bulkByScrollResponse == null){
            return;
        }
        //获取总耗时
        TimeValue timeValue = bulkByScrollResponse.getTook();
        long time = timeValue.getMillis();

        //检查请求是否超市时
        boolean timeOut = bulkByScrollResponse.isTimedOut();

        //获取已经处理的文档总数
        long totalDocs = bulkByScrollResponse.getTotal();

        //已经更新的文档数
        long updatedDocs = bulkByScrollResponse.getUpdated();

        //已经创建的文档数
        long createdDocs = bulkByScrollResponse.getCreated();
        //已经删除的文档数
        long deletedDocs = bulkByScrollResponse.getDeleted();

        //已经执行的批次数
        long batches = bulkByScrollResponse.getBatches();



    }

    private void  processBulkResponse(BulkResponse bulkResponse){
        if(bulkResponse == null)
        {
            return;
        }

        BulkItemResponse[] items = bulkResponse.getItems();
        for(BulkItemResponse response : items){
            DocWriteResponse itemres = response.getResponse();
            switch (response.getOpType()){
                case INDEX:
                case CREATE:
                    IndexResponse indexResponse = (IndexResponse) itemres;
                    System.out.println(indexResponse.getIndex()+",id="+indexResponse.getId()+",version="+indexResponse.getVersion());
                    break;
                case UPDATE:
                    UpdateResponse updateResponse = (UpdateResponse) itemres;
                    break;
                case DELETE:
                    DeleteResponse deleteResponse = (DeleteResponse) itemres;
                    break;

            }
        }

    }


    /**
     * 设置文档json格式
     * @param indexName
     * @param document
     * @param source
     */
    public void buildIndexRequestWithString(String indexName, String document, String source){
        //设置索引名称
        IndexRequest request = new IndexRequest(indexName);
        //设置文档id
        request.id(document);
        Map<String,Object> sourceMap = new HashMap<>();
        sourceMap.put("name","haha");
        sourceMap.put("valule","dd");
        request.source(sourceMap);
        ActionListener listener = new ActionListener() {
            @Override
            public void onResponse(Object o) {
                System.out.println(o);
            }

            @Override
            public void onFailure(Exception e) {
                System.out.println(e);
            }
        };
        try {
            InitEs.restClient.index(request,RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
//        InitEs.restClient.indexAsync(request, RequestOptions.DEFAULT,listener);
    }

    /**
     * 设置文档用map形式
     * @param indexName
     * @param document
     * @param source
     */
    public void buildIndexRequestWithString(String indexName, String document, Map<String,Object> source){
        //设置索引名称
        IndexRequest request = new IndexRequest(indexName);
        //设置文档id
        request.id(document);
        request.source(source);
    }

    public void buildIndexRequestWithContent(String indexName, String document){
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("name","value");
            builder.field("userName","liuran");
            builder.field("age",18);
            builder.timeField("postDate",new Date());
            builder.endObject();
            IndexRequest indexRequest = new IndexRequest(indexName).id(document).source(builder);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
