package bigdata.spark_core.etl.util;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;


public class LogParser {

//    private static String ipDbPath = "D:\\code\\bigdata\\ip2region\\ip2region\\data\\ip2region.db";
    private static String ipDbPath = "/home/jackson/app/bigdata_dw/lib/ip2region.db";

    static Access access = new Access();
    public static String  etlParse(String log){

        //记录总记录数
//        context.getCounter("etl","totalData").increment(1L);
//        String log = "[24/Aug/2020:17:14:39 +0800]\t221.7.62.0\t-\t89163\t\"-\"\tPOST\thttps://www.941jacksonhao123.com/?htt=f7\t200\t827\t9080\tHIT\tMozilla/5.0 (compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/)\ttext/html";


        try {

            if (null == log){
                throw new Exception();
            }
            String[] splits = log.split("\t");

            String time = splits[0];
            String ip = splits[1];;
            String proxyIp = splits[2];;
            long responseTime= Long.parseLong(splits[3]);
            String referer= splits[4];
            String method= splits[5];
            String url= splits[6];
            long httpCode= Long.parseLong(splits[7]);
            long requestSize= Long.parseLong(splits[8]);

            long responseSize= Long.parseLong(splits[9]);

            String cache= splits[10];

            //解析域名

            String domain;
            String path;

            String[] urlSplits = url.split("://");
            access.setHttp(urlSplits[0]);

            //www.941jacksonhao123.com/?htt=f7/fd 只分成2份，domain和path
            String[] domainAndPath = urlSplits[1].split("/",2);
            access.setDomain(domainAndPath[0]);
            access.setPath(domainAndPath[1]);


            //解析IP
            String ipMessage = IPUtil.parseIP(ip,ipDbPath);

            //0|0|0|0|内网IP|内网IP|16392
            if(ipMessage.contains("内网IP")){
//                System.out.println("内网IP");
//                context.getCounter("etl","ip属于内网ip的error").increment(1L);
                throw new Exception();
            }

            //995|中国|0|上海|上海市|电信|125682
            String[] iPsplit = ipMessage.split("\\|");
            access.setProvince(iPsplit[3]);
            access.setCity(iPsplit[4]);
            access.setOperator(iPsplit[5]);

            access.setIp(ip);
            access.setProxyIp(proxyIp);
            access.setResponseTime(responseTime);
            access.setReferer(referer);
            access.setMethod(method);
            access.setUrl(url);
            access.setHttpCode(httpCode);
            access.setRequestSize(requestSize);
            access.setResponseSize(responseSize);
            access.setCache(cache);

            //[24/Aug/2020:17:14:39 +0800]
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("[dd/MMM/yyy:HH:mm:ss +0800]", Locale.ENGLISH);
            Date date = simpleDateFormat.parse(time);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            int year = calendar.get(Calendar.YEAR);
            int month = calendar.get(Calendar.MONTH) + 1;
            int day = calendar.get(Calendar.DAY_OF_MONTH);

            access.setYear(year + "");
            access.setMonth(month < 10 ?"0" + month : month + "");
            access.setDay(day < 10 ?"0" + day : day + "");

        } catch (Exception e) {
            //记录错误的脏数据记录数
//            context.getCounter("etl","errorsData").increment(1L);
            return null;
        }

//        context.getCounter("etl","nomalData").increment(1L);
        return access.toString();
//        System.out.println(access.toString());





    }
}
