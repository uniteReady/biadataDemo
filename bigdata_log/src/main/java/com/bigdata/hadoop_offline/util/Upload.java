package com.bigdata.hadoop_offline.util;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;


public class Upload {
    public static void upload(String path,String log){

        try {
            URL url = new URL(path);
            HttpURLConnection connection = (HttpURLConnection)url.openConnection();

            connection.setRequestMethod("POST");
            connection.setDoOutput(true);


            //这里需要加上这个，否认服务端获取到的不是文本格式，如果要上传的json格式 ，要设置"application/json"
            connection.setRequestProperty("Content-Type","application/text");

            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(log.getBytes());

            outputStream.flush();
            outputStream.close();

            int responseCode = connection.getResponseCode();
            System.out.println(responseCode);


        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
