package com.example.test;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import gala2.read;
import gala2.calculate;

import java.io.IOException;
import java.util.HashMap;
import scala.Long;
import scala.util.parsing.json.JSONArray;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections.map.HashedMap;
import com.alibaba.fastjson.JSON;

import java.util.List;

@RestController

public class testj {

    @ResponseBody

    @CrossOrigin
    @RequestMapping(value = "/login", method = RequestMethod.POST)
    public HashMap<String, Object> getByJSON(@RequestBody JSONObject jsonParam) {
        // 直接将json信息打印出来

        String a=jsonParam.getString("userName");
        String b=jsonParam.getString("passWord");
        String res=read.getperson(a,b);
        res=res.substring(1,res.length()-1);
        HashMap<String, Object> hashMap=JSON.parseObject(res, HashMap.class);
        return hashMap;
    }



    @CrossOrigin
    @RequestMapping(value = "/gender",method = RequestMethod.GET)
    public HashMap<String, Object> gender()
    {
        System.out.println(read.getgender());
        HashMap<String, Object> hashMap=JSON.parseObject(read.getgender(), HashMap.class);
        return hashMap;
    }

    @CrossOrigin
    @RequestMapping(value = "/job",method = RequestMethod.GET)
    public HashMap<String, Object> job()
    {
        HashMap<String, Object> hashMap=JSON.parseObject(read.getjob(), HashMap.class);
        return hashMap;
    }

    @CrossOrigin
    @RequestMapping(value = "/birth",method = RequestMethod.GET)
    public HashMap<String, Object> birth()
    {
        HashMap<String, Object> hashMap=JSON.parseObject(read.getbirth(), HashMap.class);
        return hashMap;
    }



//------------------------------------------------------------------------------------
    @RequestMapping("/hello")
    public String index() {
        return "Hello World";
    }
    @RequestMapping("/gendercal")
    public int gendercal()
    {
        //   String a = gala2.newTask_do2();
        calculate.gender_cal();
        return 1;
    }
    @RequestMapping("/jobcal")
    public int jobcal()
    {
        //   String a = gala2.newTask_do2();
        calculate.job_cal();
        return 2;
    }
    @RequestMapping("/birthcal")
    public int birthcal()
    {
        //   String a = gala2.newTask_do2();
        calculate.birth_cal();
        return 3;
    }

/*
    @RequestMapping("/getgender")
    public HashMap<String, String> getgender()
    {
        String a = read.getgender();
        System.out.println(a);
        HashMap<String, String> hashmap = new HashMap<String, String>();
        hashmap.put("data", a);
        return hashmap;
    }*/

    @RequestMapping("/getgender_test")
    public String getgender()
    {
        String a = read.getgender();
        return a;
    }

    @RequestMapping("/getgendermap")
    public HashMap<String, Object> getgendermap()
    {
        HashMap<String, Object> hashMap=JSON.parseObject(read.getgender(), HashMap.class);
        return hashMap;
    }

    @RequestMapping("/getjob_test")
    public String getjob()
    {
        String job = read.getjob();
        return job;
    }

    @RequestMapping("/getbirth_test")
    public String getbirth()
    {
        String birth = read.getbirth();
        return birth;
    }



}