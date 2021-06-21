package com.example.test;

import org.springframework.web.bind.annotation.*;
import gala2.tests;
@RestController
public class testj {
    @RequestMapping("/hello")
    public String index() {
        return "Hello World";
    }
    @RequestMapping("/h")
    public String test()
    {
        //   String a = gala2.newTask_do2();
        String tmpa=tests.newTask_do2();
        return tmpa;
    }

/*
    @RequestMapping(value = "/get",produces = MediaType.IMAGE_JPEG_VALUE)
    @ResponseBody    //http://localhost:8080/get?name=1
    public byte[] getImage(@RequestParam("name") String name) throws IOException {
        System.out.println(c+"\\img\\"+name);
        File file = new File(c+"\\img\\"+name);
        FileInputStream inputStream = new FileInputStream(file);
        byte[] bytes = new byte[inputStream.available()];
        inputStream.read(bytes, 0, inputStream.available());
        return bytes;
    }
    */


}