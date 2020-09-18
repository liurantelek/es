package com.liuran.es.esapidemo.demo1.serviceImpl;

import com.liuran.es.esapidemo.demo1.init.InitEs;
import com.liuran.es.esapidemo.demo1.service.HighEsService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.stereotype.Service;

import java.io.IOException;
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
