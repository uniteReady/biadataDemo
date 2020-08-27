package com.bigdata.hadoop_offline.util;


import com.amazonaws.services.dynamodbv2.xspec.S;
import org.lionsoul.ip2region.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;

public class IPUtil {
    private static Logger logger = LoggerFactory.getLogger(IPUtil.class);

    public static String parseIP(String ip, String path) {

        if (null == path) {
            System.out.println("| Usage: java -jar ip2region-{version}.jar [ip2region db file]");
            System.exit(0);
//            return ;
        }

        File file = new File(path);
        if (file.exists() == false) {
            System.out.println("Error: Invalid ip2region.db file");
            System.exit(0);
//            return;
        }

        int algorithm = DbSearcher.BTREE_ALGORITHM;
        //算法可以传参确定，这里写死
        String algoName = "B-tree";
        if (null != algoName) {
            if (algoName.equalsIgnoreCase("binary")) {
                algoName = "Binary";
                algorithm = DbSearcher.BINARY_ALGORITHM;
            } else if (algoName.equalsIgnoreCase("memory")) {
                algoName = "Memory";
                algorithm = DbSearcher.MEMORY_ALGORITYM;
            }
        }

        DataBlock dataBlock = null;

        try {
            DbConfig config = new DbConfig();
            DbSearcher searcher = new DbSearcher(config, path);


            //define the method
            Method method = null;
            switch (algorithm) {
                case DbSearcher.BTREE_ALGORITHM:
                    method = searcher.getClass().getMethod("btreeSearch", String.class);
                    break;
                case DbSearcher.BINARY_ALGORITHM:
                    method = searcher.getClass().getMethod("binarySearch", String.class);
                    break;
                case DbSearcher.MEMORY_ALGORITYM:
                    method = searcher.getClass().getMethod("memorySearch", String.class);
                    break;
            }



            if (Util.isIpAddress(ip) == false) {
                logger.info("Error: Invalid ip address");
                logger.info("Invalid ip : " + ip);
            }


            dataBlock = (DataBlock) method.invoke(searcher, ip);

            logger.info("dataBlock: " + dataBlock.toString());
//            System.out.println(dataBlock.toString());

            searcher.close();


        } catch (Exception e) {
            e.printStackTrace();
        }


        return dataBlock.toString();
    }

}
