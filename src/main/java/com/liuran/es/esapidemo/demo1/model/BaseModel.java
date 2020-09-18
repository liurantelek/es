package com.liuran.es.esapidemo.demo1.model;

import org.elasticsearch.search.DocValueFormat;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * @version v1.0
 * @ProjectName: es-api-demo
 * @ClassName: BaseModel
 * @Description: TODO(一句话描述该类的功能)
 * @Author: 刘然
 * @Date: 2020/9/18 9:37
 */
public abstract class BaseModel implements Model {


    public Map<String,Object> covertModelToMap(){
        Field[] declaredFields = BaseModel.class.getDeclaredFields();
        Map<String,Object> rowMap = new HashMap<>();
        for(Field f : declaredFields){
            f.setAccessible(true);
            try {
                Object o = f.get(this);
                Class<?> type = f.getType();
                if(type == String.class){
                    o = o.toString();
                }else if(type == Integer.class || type == int.class){

                }else if(type == Double.class || type == double.class){

                }else if(type == Long.class || type == long.class){

                }else if(type == BigDecimal.class ){

                }else {

                }
            } catch (IllegalAccessException e) {
                System.out.println("获取"+this.getClass().getSimpleName()+"的"+f.getName()+"属性异常");
            }
        }
        return rowMap;
    }

}
