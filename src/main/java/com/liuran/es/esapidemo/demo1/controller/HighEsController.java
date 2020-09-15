package com.liuran.es.esapidemo.demo1.controller;

import com.liuran.es.esapidemo.demo1.service.HighEsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @version v1.0
 * @ProjectName: es-api-demo
 * @ClassName: HighEsController
 * @Description: TODO(一句话描述该类的功能)
 * @Author: 刘然
 * @Date: 2020/9/15 18:24
 */
@RequestMapping("/es/high")
@Controller
@ResponseBody
public class HighEsController {

    @Autowired
    private HighEsService highEsService;

    @RequestMapping("/index/put")
    public String putIndexHighEs(String indexName,String document){
        if(StringUtils.isEmpty(indexName) || StringUtils.isEmpty(document)){
            return "parameter are error";
        }
        highEsService.indexDocuments(indexName,document);
        return "index High Es successed";
    }
}