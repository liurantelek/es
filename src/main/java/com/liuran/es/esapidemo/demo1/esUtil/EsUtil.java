package com.liuran.es.esapidemo.demo1.esUtil;

import com.liuran.es.esapidemo.demo1.init.InitEs;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Map;

/**
 * @version v1.0
 * @ProjectName: es-api-demo
 * @ClassName: EsUtil
 * @Description: TODO(一句话描述该类的功能)
 * @Author: 刘然
 * @Date: 2020/9/16 18:20
 */
public class EsUtil {

    public static boolean checkExistsIndexAndDocument(String indexName,String document){
        GetRequest getRequest = new GetRequest(indexName,document);
        //禁用提取源
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        //禁用提取存储字段
        getRequest.storedFields("_none");
        boolean exists = false;
        try {
             exists = InitEs.restClient.exists(getRequest, RequestOptions.DEFAULT);
            if(exists){
                System.out.println("存在索引为"+indexName+",document为"+document);
            }else {
                System.out.println("改索引下不存在该document");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            InitEs.closeEs();
        }
        return exists;
    }


    /**
     * 异步检验文档是否存在
     * @param indexName
     * @param document
     */
    public static void checkExistsIndexAndDocumentAsync(String indexName,String document){
        GetRequest getRequest = new GetRequest(indexName,document);
        //禁用提取源
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        //禁用提取存储字段
        getRequest.storedFields("_none");

        ActionListener<Boolean> listener = new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean exists) {

                System.out.println("索引："+indexName+"下的"+document+"文档存在性是"+exists);
            }

            @Override
            public void onFailure(Exception e) {
                System.out.println(e);
            }
        } ;

        InitEs.restClient.existsAsync(getRequest,RequestOptions.DEFAULT,listener);
        InitEs.closeEs();
    }

    public static DeleteRequest  deleteIndexAndRequest(String indexName,String document){
        DeleteRequest deleteRequest = new DeleteRequest(indexName,document);
        //设置路由
        deleteRequest.routing("routing");
        //设置超时时间
        deleteRequest.timeout(TimeValue.timeValueMinutes(2));
        deleteRequest.timeout("2m");
        //设置刷新策略
        deleteRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        deleteRequest.setRefreshPolicy("wait_for");
        //设置版本
        deleteRequest.version(2);
        deleteRequest.versionType(VersionType.EXTERNAL);
        return deleteRequest;
    }

    /**
     * 删除指定index下的document
     * @param indexName
     * @param document
     */
    public static DeleteResponse deleteIndexDocument(String indexName,String document){
        DeleteRequest deleteRequest = deleteIndexAndRequest(indexName, document);
        DeleteResponse response =null;
        try {
             response = InitEs.restClient.delete(deleteRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            InitEs.closeEs();
        }
        return response;
    }

    public static DeleteResponse deleteIndexDocumentAsync(String indexName,String document){

        DeleteRequest deleteRequest = deleteIndexAndRequest(indexName, document);
        ActionListener listener = new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse response) {

                System.out.println(response.getId());
                System.out.println(response.getIndex());
                System.out.println(response.getVersion());

            }

            @Override
            public void onFailure(Exception e) {

            }
        };
        try {
            InitEs.restClient.deleteAsync(deleteRequest,RequestOptions.DEFAULT,listener);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            InitEs.closeEs();
        }
        return null;
    }

    public void deleteIndexDocuments(String indexName,String document){
        DeleteResponse response = deleteIndexDocument(indexName, document);
        ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
        if(shardInfo.getTotal()!= shardInfo.getSuccessful()){
            System.out.println("success shards are not enough");
        }
        if(shardInfo.getFailed()>0){
            for(ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()){
                //打印失败原因
                System.out.println(failure.reason());
            }
        }

    }

    public static UpdateRequest buildUpdateRequestIndexDocument(String indexName,String document){
        UpdateRequest request = new UpdateRequest(indexName,document);
        request.routing("routing");
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("is");
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        request.setRefreshPolicy("wait_for");
        //设置：如果更新的文档在更新时被另一个操作更改，则重试更新操作的次数
        request.retryOnConflict(3);
        //启用源检索,在默认情况下禁用
        request.fetchSource(true);
        //为特定字段配置源包含关系
        String [] includes = new String[]{"updated","t*"};
        String [] excludes = Strings.EMPTY_ARRAY;
        request.fetchSource(new FetchSourceContext(true,includes,excludes));
        //为特定字段配置源排除关系
         includes = Strings.EMPTY_ARRAY;;
        excludes = new String[]{"updated"};
        request.fetchSource(new FetchSourceContext(true,includes,excludes));
        return request;
    }

    public static void updateDocument(String indexName, String document, Map<String,Object> source){
        UpdateRequest request = buildUpdateRequestIndexDocument(indexName, document);
        request.doc(source, XContentType.JSON);
    }


}
