package com.liuran.es.esapidemo.demo1.serviceImpl;

import com.liuran.es.esapidemo.demo1.init.InitEs;
import com.liuran.es.esapidemo.demo1.service.HighEsService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
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
