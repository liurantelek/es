package com.liuran.es.esapidemo.demo1.service;

/**
 * @Auther: 45417
 * @Date: 2020/9/15 18:25
 * @Description:
 */
public interface HighEsService {
    String indexDocuments(String indexName, String document);

    String getRequest(String indexName, String document,String id);

    void executeBulkRequest(String indexName, String field);

    void bulkProcess(String indexName, String field);

    void executeReindex(String fromIndex, String toIndex);
}
