package com.bigdata.springbootdemo.controller;

import com.bigdata.springbootdemo.bean.Student;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DemoController {
    @Value("${testFiels}")
    private String testFiels;
    @Autowired
    Student student;
    @RequestMapping("/get")
    public String show(){
        return student.getName() + " " + student.getAdress() + testFiels;
    }

}
