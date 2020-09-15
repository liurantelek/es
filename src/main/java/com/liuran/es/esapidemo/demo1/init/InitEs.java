package com.liuran.es.esapidemo.demo1.init;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;

/**
 * @version v1.0
 * @ProjectName: es-api-demo
 * @ClassName: InitEs
 * @Description: TODO(一句话描述该类的功能)
 * @Author: 刘然
 * @Date: 2020/9/15 18:31
 */

@Service
public class InitEs {

    private static final Logger log = LoggerFactory.getLogger(InitEs.class);

    public static RestHighLevelClient restClient;

    @PostConstruct
    public void InitEs(){
        restClient = new RestHighLevelClient(RestClient.builder(new HttpHost("123.57.20.251",9300,"http")));
        log.info("elasticSearch init suscceed");

    }

    public static void closeEs(){
        try {
            restClient.close();
        } catch (IOException e) {
            log.error("close esClient error");
        }
    }
}
