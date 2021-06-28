package com.example.test;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import gala2.read;
import gala2.colony_read;
import gala2.calculate;
import gala2.test;
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
    @RequestMapping(value = "/read_by_label", method = RequestMethod.POST)
    public String read_by_label(@RequestBody JSONObject jsonParam) {
        String gender=jsonParam.getString("gender");
        String birth=jsonParam.getString("birth");
        String job=jsonParam.getString("job");
        String shopping_cycle=jsonParam.getString("shopping_cycle");
        String consumption_ablity=jsonParam.getString("consumption_ablity");
        String res=read.read_by_label(gender,birth,job,shopping_cycle,consumption_ablity);
        return res;
    }


    @CrossOrigin
    @RequestMapping(value = "/read_orders_info", method = RequestMethod.POST)
    public String read_orders_info(@RequestBody JSONObject jsonParam) {
        // 直接将json信息打印出来
        String a=jsonParam.getString("memberid");
        String res=read.read_orders_info(a);
        return res;
    }


    @CrossOrigin
    @RequestMapping(value = "/test", method = RequestMethod.GET)
    public String test() {
        // 直接将json信息打印出来
     //   String a=jsonParam.getString("memberid");
        String res=read.read_by_label("男","90后","all","all","all");
        return res;
    }

    @CrossOrigin
    @RequestMapping(value = "/user_discount", method = RequestMethod.GET)
    public String discount(@RequestBody JSONObject jsonParam) {
        String a=jsonParam.getString("username");
        String res=read.user_discount(a);
        return res;
    }


    @CrossOrigin
    @RequestMapping(value = "/login", method = RequestMethod.POST)
    public String getByJSON(@RequestBody JSONObject jsonParam) {
        // 直接将json信息打印出来
        String a=jsonParam.getString("userName");
        String b=jsonParam.getString("passWord");
        if(a=="admin"&&b=="admin")
        {
            return "admin";
        }
        else
        {
            String res=read.getperson(a,b);
            res=res.substring(1,res.length()-1);
          //  HashMap<String, Object> hashMap=JSON.parseObject(res, HashMap.class);
            return res;
        }

    }

    @CrossOrigin
    @RequestMapping(value = "/search", method = RequestMethod.POST)
    public HashMap<String, Object> search(@RequestBody JSONObject jsonParam) {
        // 直接将json信息打印出来
        String a=jsonParam.getString("keyWords");
        String res=read.search(a);
        res=res.substring(1,res.length()-1);
        HashMap<String, Object> hashMap=JSON.parseObject(res, HashMap.class);
        return hashMap;
    }
/*
    @CrossOrigin
    @RequestMapping(value = "/getpay", method = RequestMethod.POST)
    public HashMap<String, Object> getpay(@RequestBody JSONObject jsonParam) {
        // 直接将json信息打印出来
        String a=jsonParam.getString("keyWords");
        String res=read.getpay(a);
        res=res.substring(1,res.length()-1);
        HashMap<String, Object> hashMap=JSON.parseObject(res, HashMap.class);
        return hashMap;
    }*/
    @CrossOrigin
    @RequestMapping(value = "/birth_brand_preference", method = RequestMethod.GET)
    public String birth_brand_preference() {
        String res=read.birth_brand_preference();
        return res;
    }
    @CrossOrigin
    @RequestMapping(value = "/birth_payment_way", method = RequestMethod.GET)
    public String birth_payment_way() {
        String res=read.birth_payment_way();
        return res;
    }

    @CrossOrigin
    @RequestMapping(value = "/birth_shopping_cycle", method = RequestMethod.GET)
    public String birth_shopping_cycle() {
        String res=read.birth_shopping_cycle();
        return res;
    }
    @CrossOrigin
    @RequestMapping(value = "/birth_ave_price_range", method = RequestMethod.GET)
    public String birth_ave_price_range() {
        String res=read.birth_ave_price_range();
        return res;
    }

    @CrossOrigin
    @RequestMapping(value = "/birth_order_highest_range", method = RequestMethod.GET)
    public String birth_order_highest_range() {
        String res=read.birth_order_highest_range();
        return res;
    }

    @CrossOrigin
    @RequestMapping(value = "/birth_frequency_range", method = RequestMethod.GET)
    public String birth_frequency_range() {
        String res=read.birth_frequency_range();
        return res;
    }

    @CrossOrigin
    @RequestMapping(value = "/birth_exchange_item_rate", method = RequestMethod.GET)
    public String birth_exchange_item_rate() {
        String res=read.birth_exchange_item_rate();
        return res;
    }

    @CrossOrigin
    @RequestMapping(value = "/birth_return_item_rate", method = RequestMethod.GET)
    public String birth_return_item_rate() {
        String res=read.birth_return_item_rate();
        return res;
    }


/*
    @CrossOrigin
    @RequestMapping(value = "/test2",method = RequestMethod.GET)
    public String test2()
    {
        return colony_read.test2();
    }
*/

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

    @CrossOrigin
    @RequestMapping(value = "/nationality",method = RequestMethod.GET)
    public HashMap<String, Object> nationality()
    {
        HashMap<String, Object> hashMap=JSON.parseObject(read.getnationality(), HashMap.class);
        return hashMap;
    }

    @CrossOrigin
    @RequestMapping(value = "/political_face",method = RequestMethod.GET)
    public HashMap<String, Object> political_face()
    {
        HashMap<String, Object> hashMap=JSON.parseObject(read.getpolitical_face(), HashMap.class);
        return hashMap;
    }

    @CrossOrigin
    @RequestMapping(value = "/brand_preference",method = RequestMethod.GET)
    public HashMap<String, Object> brand_preference()
    {
        HashMap<String, Object> hashMap=JSON.parseObject(read.getbrand_preference(), HashMap.class);
        return hashMap;
    }

    @CrossOrigin
    @RequestMapping(value = "/marriage",method = RequestMethod.GET)
    public HashMap<String, Object> marriage()
    {
        HashMap<String, Object> hashMap=JSON.parseObject(read.getmarriage(), HashMap.class);
        return hashMap;
    }


    @CrossOrigin
    @RequestMapping(value = "/consumption_ablity",method = RequestMethod.GET)
    public HashMap<String, Object> consumption_ablity()
    {
        HashMap<String, Object> hashMap=JSON.parseObject(read.getconsumption_ablity(), HashMap.class);
        return hashMap;
    }

    @CrossOrigin
    @RequestMapping(value = "/shopping_cycle",method = RequestMethod.GET)
    public HashMap<String, Object> shopping_cycle()
    {
        HashMap<String, Object> hashMap=JSON.parseObject(read.getshopping_cycle(), HashMap.class);
        return hashMap;
    }

    @CrossOrigin
    @RequestMapping(value = "/ave_price_range",method = RequestMethod.GET)
    public HashMap<String, Object> ave_price_range()
    {
        HashMap<String, Object> hashMap=JSON.parseObject(read.getave_price_range(), HashMap.class);
        return hashMap;
    }

    @CrossOrigin
    @RequestMapping(value = "/order_highest_range",method = RequestMethod.GET)
    public HashMap<String, Object> order_highest_range()
    {
        HashMap<String, Object> hashMap=JSON.parseObject(read.getorder_highest_range(), HashMap.class);
        return hashMap;
    }

    @CrossOrigin
    @RequestMapping(value = "/log_frequency",method = RequestMethod.GET)
    public HashMap<String, Object> log_frequency()
    {
        HashMap<String, Object> hashMap=JSON.parseObject(read.getlog_frequency(), HashMap.class);
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

    @RequestMapping("/gettest")
    public int birth_shop()
    {
        //   String a = gala2.newTask_do2();
       // calculate.birth_colony_shopping_cycle();
        return 4;
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
        String birth = read.gettest();

        return birth;
    }



}