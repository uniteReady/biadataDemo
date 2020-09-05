package com.bigdata.log_server.controller;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;



@RestController
public class LogController {

    //注意依赖和导包
    private static final Logger logger = Logger.getLogger("LogController");

@PostMapping(value = "/upload")
public void upload(@RequestBody String log){
    logger.info(log);
    System.out.println(log);
}
}
