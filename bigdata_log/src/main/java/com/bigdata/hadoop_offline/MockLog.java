package com.bigdata.hadoop_offline;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Random;


public class MockLog {

    private static final SimpleDateFormat SDF =new SimpleDateFormat("[dd/MMM/yyy:HH:mm:ss +0800]", Locale.ENGLISH);
    private static String[] IP = new String[]{
            "202.38.150.0","202.38.170.0","202.97.32.0","219.142.00","210.77.32.0",
            "218.30.86.0","124.112.0.0","61.133.128.0","220.178.0.0","60.166.0.0",
            "121.204.0.0","218.66.0.0","59.56.00","61.131.0.0","218.86.0.0",
            "202.97.16.0","221.7.62.0","218.30.167.0","61.128.0.0","125.76.0.0",
            "119.0.0.0","114.138.0.0","58.42.0.0","61.189.128.0",
            "121.58.0.0","210.168.1.0","121.8.0.0","218.16.0.0","58.60.0.0",
            "116.10.0.0","220.173.0.0","219.148.0.0","221.14.243.0","61.167.6.0",
            "59.172.00","61.137.0.0","219.150.0.0","218.4.0.0","59.62.0.0",
            "123.184.0.0","222.74.0.0","122.4.0.0","59.48.0.0","121.59.0.0",
            "61.129.0.0","218.88.0.0","220.128.0.0","58.43.0.0","218.0.0.0",
            "61.138.195.0","124.118.0.0","61.92.0.0","219.151.64.0","60.29.11.0"
    };
    private static String[] METHOD = new String[]{"POST","GET"};
    private static String[] URL = new String[]{
            "http://www.941jackson.com/","https://www.941jacksonsina.com.cn/",
            "https://www.941jacksonhao123.com/","https://www.941jacksonsohu.com/","https://www.941jackson163.com/",
            "https://www.941jacksonjd.com/","https://941jackson.ai.taobao.com/","http://www.941jacksonhao123.com/tejia/",
            "http://www.941jacksonbaidu.com.cn/icbc/","https://www.941jacksonbilibili.com/"
    };
    private static String[] URL_SUFFIX = new String[]{
            "?ab=3&c=2&d=4","?m=7&method=ff","a=5&b=2&c=3",
            "?auth=afe&file=de","?htt=f7","?y=8&u=6"
    };
    private static String[] CACHE = new String[]{"MISS","HIT"};
    private static String[] HTTPCODE = new String[]{"200","404","500","200","200"};



    public static void main(String[] args) throws Exception {



//        String outPath = args[0];

        String outPath = "/access_20200826.log";

        Random random = new Random();


        BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(
                        new FileOutputStream(
                                new File("data/log" + outPath))));


        for(int i = 0; i <= 400; i++){
            //日志开始时间
            String logStartTime = SDF.format(new Date());
            //访问ip
            String ip = IP[random.nextInt(IP.length)];
            //代理ip
            String proxyIP = "-";
            //responsetime（单位：ms）
            String responsetime = String.valueOf(random.nextInt(100000));
            //referer
            String referer = "-";
            //method
            String method = METHOD[random.nextInt(METHOD.length)];
            //访问url
            String url = URL[random.nextInt(URL.length)] + URL_SUFFIX[random.nextInt(URL_SUFFIX.length)];
            //httpcode
            String httpcode = HTTPCODE[random.nextInt(HTTPCODE.length)];
            //requestsize
            String requestsize = String.valueOf(random.nextInt(1000));
            //responsesize
            String responsesize = getResponseSize(random);
            //cache命中状态
            String cacheState = CACHE[random.nextInt(CACHE.length)];
            //UA头
            String ua = "Mozilla/5.0 (compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/)";
            //文件类型
            String fileType = "text/html";


            String result = logStartTime + "\t" + ip + "\t" + proxyIP + "\t" + responsetime + "\t" + referer + "\t" +
                    method + "\t" + url + "\t" + httpcode + "\t" + requestsize + "\t" + responsesize +
                    "\t" + cacheState + "\t" + ua + "\t" + fileType ;
            writer.write(result);

            writer.newLine();
        }
        writer.flush();
        writer.close();


    }



    private static String getLogStartTime(SimpleDateFormat sdf, Long newLonglogStartTime) {
        return sdf.format(new Date(newLonglogStartTime));
    }

    private static String getResponseSize(Random random) {
        String right = String.valueOf(random.nextInt(10000));
        String wrong1 = "-";
        String wrong2 = "脏数据";
        int flag = random.nextInt(1000);
        if(flag % 20 == 0) {
            return wrong1;
        } else if(flag % 21 == 0) {
            return wrong2;
        } else {
            return right;
        }

    }


}
