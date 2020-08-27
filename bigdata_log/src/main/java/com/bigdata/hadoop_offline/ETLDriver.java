package com.bigdata.hadoop_offline;

import com.bigdata.hadoop_offline.util.DateUtil;
import com.bigdata.hadoop_offline.util.FileUtils;
import com.bigdata.hadoop_offline.util.IPUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.regex.Pattern;

public class ETLDriver {

    private static String path = "D:\\code\\bigdata\\ip2region\\ip2region\\data\\ip2region.db";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String input = "data\\log\\testLog";
        String output = "out";

        // 1 获取Job
        Configuration configuration = new Configuration();
//        configuration.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR,",");

        Job job = Job.getInstance(configuration);


        FileUtils.deleteOutput(configuration, output);

        // 2 设置主类
        job.setJarByClass(ETLDriver.class);

        // 3 设置Mapper和Reducer
        job.setMapperClass(ETLMapper.class);
//        job.setReducerClass(MyReducer.class);

        // 4 设置Mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5 设置Reduce阶段输出的key和value类型
//        job.setOutputKeyClass(NullWritable.class);
//        job.setOutputValueClass(Access.class);

        // 6 设置输入和输出路径
//        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 7 提交Job
        boolean result = job.waitForCompletion(true);
        System.exit(result? 0:1 );


    }




    public static class ETLMapper extends Mapper<LongWritable,Text,Text, NullWritable> {


        Access access = new Access();
        Text text = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          //[18/Aug/2020:14:18:57 +0800]	125.76.0.0	-	23293	"-"	"GET    http://www.941jacksonhao123.com/tejia/?ab=3&c=2&d=4"	200	573	6959	MISS	"Mozilla/5.0 (compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/)"	"text/html"

            String[] splits = value.toString().split("\t");

            //解析时间
            SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            SimpleDateFormat sdf2 =new SimpleDateFormat("[dd/MMM/yyy:HH:mm:ss +0800]", Locale.ENGLISH);

            try {
                String date = DateUtil.parseDate(sdf2,sdf1,splits[0]);
                access.setStartTime(date);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            
            access.setIp(splits[1]);
            access.setProxyIp(splits[2]);
            access.setResponsetime(Integer.parseInt(splits[3]));
            access.setReferer(splits[4]);
            access.setMethod(splits[5].replace("\"",""));
            access.setUrl(splits[6].replace("\"",""));
            access.setHttpcode(Integer.parseInt(splits[7]));
            access.setRequestsize(Integer.parseInt(splits[8]));
            if(Pattern.matches("[0-9]+",splits[9])){
                //清洗不是数字的responsesize
                access.setResponsesize(Integer.parseInt(splits[9]));
            }else {
                return;
            }


            access.setCache(splits[10]);
            access.setUa(splits[11]);
            access.setFileType(splits[12]);

            //解析IP
            String ipMessage = IPUtil.parseIP(access.getIp(), path);

            //0|0|0|0|内网IP|内网IP|16392
            if(ipMessage.contains("内网IP")){
                return;
            }

            //995|中国|0|上海|上海市|电信|125682
            String[] iPsplit = ipMessage.split("\\|");
            access.setCountry(iPsplit[1]);
            access.setProvince(iPsplit[3]);
            access.setCity(iPsplit[4]);
            access.setOperator(iPsplit[5]);

            text.set(access.toString());
            context.write(text,NullWritable.get());



        }



        }


    }



//
//    public static class MyReducer extends Reducer<Text,Access, NullWritable,Access> {
//        @Override
//        protected void reduce(Text key, Iterable<Access> values, Context context) throws IOException, InterruptedException {
//            long upload = 0;
//            long download = 0;
//            for (Access value : values) {
//                upload += value.getUpload();
//                download += value.getDownload();
//            }
//
//            context.write(NullWritable.get(),new Access(key.toString(),upload,download));
//        }
//    }
//}
