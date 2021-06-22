package com.example.test;

import org.springframework.web.bind.annotation.*;
import gala2.read;
import gala2.calculate;
@RestController
public class testj {
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


    @RequestMapping("/getgender")
    public String getgender()
    {
          String gender = read.getgender();
           return gender;
    }

    @RequestMapping("/getjob")
    public String getjob()
    {
        String job = read.getjob();
        return job;
    }

    @RequestMapping("/getbirth")
    public String getbirth()
    {
        String birth = read.getbirth();
        return birth;
    }



}