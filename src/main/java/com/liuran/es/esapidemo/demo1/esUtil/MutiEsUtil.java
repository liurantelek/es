package com.liuran.es.esapidemo.demo1.esUtil;

import com.liuran.es.esapidemo.demo1.init.InitEs;
import com.liuran.es.esapidemo.demo1.model.BaseModel;
import com.liuran.es.esapidemo.demo1.model.Model;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;

import java.util.Map;

/**
 * @version v1.0
 * @ProjectName: es-api-demo
 * @ClassName: MutiEsUtil
 * @Description: es的操作类
 * @Author: 刘然
 * @Date: 2020/9/17 17:40
 */
public class MutiEsUtil {

    public static IndexRequest buildIndexRequest(String indexName){
        return  new IndexRequest(indexName);
    }

    public static void putSourceToIndexRequest(IndexRequest indexRequest,String document,Map<String,Object> sourceMap){
        indexRequest.id(document).source(sourceMap);
    }

    public static void  putSourceToIndexRequest(IndexRequest request, BaseModel model){
        request.id(model.getEsId()).source(model.covertModelToMap());
    }

    /**
     * updateRow存储需要更新的字段
     * @param request
     * @param model
     * @param updateRow
     */
    public static void putSourceToIndexRequest(IndexRequest request, BaseModel model, Map<String, Object> updateRow) {
        Map<String, Object> rowMap = model.covertModelToMap();
        rowMap.putAll(updateRow);
        request.id(model.getEsId()).source(rowMap);

    }

    /**
     * 向bulkRequest添加indexRequest对象
     * @param bulkRequest
     */
    public static void addIndexRequest(IndexRequest request,BulkRequest bulkRequest ){
        bulkRequest.add(request);
    }

    /**
     * 构建bulkRequest对象
     * @return
     */
    public static BulkRequest buildBulkRequest(){
        return new BulkRequest();
    }

    public static void updateModel(String indexName,BaseModel model,Map<String, Object> updateRow){
        IndexRequest indexRequest = new IndexRequest(indexName);
        putSourceToIndexRequest(indexRequest,model,updateRow);

    }

}
