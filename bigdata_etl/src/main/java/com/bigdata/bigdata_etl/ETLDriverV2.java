package com.bigdata.bigdata_etl;

import com.bigdata.bigdata_etl.util.FileUtils;
import com.bigdata.bigdata_etl.util.LogParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.Iterator;

public class ETLDriverV2 extends Configured implements Tool {

//    private static String path = "D:\\code\\bigdata\\ip2region\\ip2region\\data\\ip2region.db";
    private static Logger logger = LoggerFactory.getLogger("ETLDriverV2");

    public static void main(String[] args) throws Exception {
//        String input = "data\\log\\testLog";
//        String output = "out";

        int run = ToolRunner.run(new Configuration(), new ETLDriverV2(), args);
        System.exit(run);

    }

    @Override
    public int run(String[] args) throws Exception {
        if(args.length != 2){
            logger.error("输入参数错误，应该为2个参数：input/output");
            logger.error("lenth: " + args.length);
            System.exit(0);
        }

        String input = args[0];
        String output = args[1];

        // 1 获取Job
        //这里注意不是new出来
        Configuration configuration = super.getConf();


        Job job = Job.getInstance(configuration);


        FileUtils.deleteOutput(configuration, output);

        // 2 设置主类
        job.setJarByClass(ETLDriverV2.class);

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

        //获取计数器
        CounterGroup counterGroup = job.getCounters().getGroup("etl");
        Iterator<Counter> iterator = counterGroup.iterator();

        while (iterator.hasNext()){
            Counter counter = iterator.next();
            System.out.println(counter.getName() + ": " + counter.getValue());
        }


        return result? 0:1 ;
    }


    public static class ETLMapper extends Mapper<LongWritable,Text,Text, NullWritable> {



        Text text = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Access access = new Access();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          //[18/Aug/2020:14:18:57 +0800]	125.76.0.0	-	23293	"-"	"GET    http://www.941jacksonhao123.com/tejia/?ab=3&c=2&d=4"	200	573	6959	MISS	"Mozilla/5.0 (compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/)"	"text/html"

            String log = value.toString();
            String logParsed = LogParser.etlParse(context, log);

            if (null != logParsed){
                text.set(logParsed);

                context.write(text,NullWritable.get());
            }

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
