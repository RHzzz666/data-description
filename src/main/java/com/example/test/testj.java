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
        String birth=jsonParam.getString("birth");
        System.out.print(birth);
        String shopping_cycle=jsonParam.getString("shopping_cycle");
        System.out.print(shopping_cycle);
        String discount=jsonParam.getString("discount");
        System.out.print(discount);
        String cast=jsonParam.getString("cast");
        System.out.println(cast);
        String res=read.read_by_label(birth,shopping_cycle,discount,cast);
        System.out.println(res);
        return res;
    }


    @CrossOrigin
    @RequestMapping(value = "/read_orders_info", method = RequestMethod.POST)
    public String read_orders_info(@RequestBody JSONObject jsonParam) {
        String a=jsonParam.getString("memberid");
        String res=read.read_orders_info(a);
        return res;
    }

    @CrossOrigin
    @RequestMapping(value = "/read_top", method = RequestMethod.POST)
    public String read_top(@RequestBody JSONObject jsonParam) {
        String a=jsonParam.getString("id");
        System.out.print(a);
        String res=read.read_top(a);
        System.out.println(res);
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
    @RequestMapping(value = "/discount", method = RequestMethod.GET)
    public String discount() {
        String res=read.dicount();
        return res;
    }

    @CrossOrigin
    @RequestMapping(value = "/cast", method = RequestMethod.GET)
    public String cast() {
        String res=read.cast();
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
            System.out.println(res);
            return res;
        }

    }

    @CrossOrigin
    @RequestMapping(value = "/search", method = RequestMethod.POST)
    public String search(@RequestBody JSONObject jsonParam) {
        // 直接将json信息打印出来
        String a=jsonParam.getString("keyWords");
        String res=read.search(a);
        res=res.substring(1,res.length()-1);
        System.out.println(res);
     //   HashMap<String, Object> hashMap=JSON.parseObject(res, HashMap.class);

        return res;
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
    @RequestMapping(value = "/test", method = RequestMethod.GET)
    public String test() {
        String res=read.read_top("1");
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
    public String gender()
    {
      //  System.out.println(read.getgender());
        String hashMap=read.getgender();
        return hashMap;
    }

    @CrossOrigin
    @RequestMapping(value = "/job",method = RequestMethod.GET)
    public String job()
    {
        String hashMap=read.getjob();
        return hashMap;
    }

    @CrossOrigin
    @RequestMapping(value = "/birth",method = RequestMethod.GET)
    public String birth()
    {
        String hashMap=read.getbirth();
        return hashMap;
    }

    @CrossOrigin
    @RequestMapping(value = "/nationality",method = RequestMethod.GET)
    public String nationality()
    {
        String hashMap=read.getnationality();
        return hashMap;
    }

    @CrossOrigin
    @RequestMapping(value = "/political_face",method = RequestMethod.GET)
    public String political_face()
    {
        String hashMap=read.getpolitical_face();
        return hashMap;
    }

    @CrossOrigin
    @RequestMapping(value = "/brand_preference",method = RequestMethod.GET)
    public String brand_preference()
    {
        String hashMap=read.getbrand_preference();
        return hashMap;
    }

    @CrossOrigin
    @RequestMapping(value = "/marriage",method = RequestMethod.GET)
    public String marriage()
    {
        String hashMap=read.getmarriage();
        return hashMap;
    }


    @CrossOrigin
    @RequestMapping(value = "/consumption_ablity",method = RequestMethod.GET)
    public String consumption_ablity()
    {
        String hashMap=read.getconsumption_ablity();
        return hashMap;
    }

    @CrossOrigin
    @RequestMapping(value = "/shopping_cycle",method = RequestMethod.GET)
    public String shopping_cycle()
    {
        String hashMap=read.getshopping_cycle();
        return hashMap;
    }

    @CrossOrigin
    @RequestMapping(value = "/ave_price_range",method = RequestMethod.GET)
    public String ave_price_range()
    {
        String hashMap=read.getave_price_range();
        return hashMap;
    }

    @CrossOrigin
    @RequestMapping(value = "/order_highest_range",method = RequestMethod.GET)
    public String order_highest_range()
    {
        String hashMap=read.getorder_highest_range();
        return hashMap;
    }

    @CrossOrigin
    @RequestMapping(value = "/log_frequency",method = RequestMethod.GET)
    public String log_frequency()
    {
        String hashMap=read.getlog_frequency();
        return hashMap;
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



}