package com.liuran.es.esapidemo.demo1.controller;


import com.liuran.es.esapidemo.demo1.service.HighEsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RequestMapping("/ndex")
@RestController
public class ReindexController {

    @Autowired
    private HighEsService highEsService;

    @RequestMapping("reindex")
    public String reindex(String fromIndex,String toIndex){
         highEsService.executeReindex(fromIndex,toIndex);
         return "成功";
    }


}
