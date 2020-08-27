import com.bigdata.hadoop_offline.util.IPUtil;

import java.util.regex.Pattern;

public class Test {
    @org.junit.Test
    public void testIPParse(){
        String[] ipArray = {"202.97.32.0" ,
                "59.48.0.0" ,
                "218.30.86.0" ,
                "59.56.00" ,
                "58.43.0.0" ,
                "59.62.0.0" ,
                "59.62.0.0" ,
                "122.4.0.0" ,
                "61.129.0.0" ,
                "218.4.0.0" ,
                "218.66.0.0" ,
                "59.62.0.0" ,
                "59.48.0.0"};
        for (String ip : ipArray) {
            IPUtil.parseIP(ip,"D:\\code\\bigdata\\ip2region\\ip2region\\data\\ip2region.db");
        }

    }

    @org.junit.Test
    public void testIsNumerical(){

        String[] arr = {"12","zneg","-","脏数据","1","348923898"};


        for (String s : arr) {
//            System.out.println(pattern.matches("[0-9]+",s));
            System.out.println(Pattern.matches("[0-9]+",s));


        }

    }

    @org.junit.Test
    public void testSplit(){
        String str = "www.fdksk.fdc/fdsk/fdfkdk/fdfdk";
        String[] split = str.split("/", 2);
        for (String s : split) {

            System.out.println(s);
            System.out.println("\n");
        }

    }

    @org.junit.Test
    public void testRex(){
        String[] arr = {"0.930","FDKF","8377"};

        for (String s : arr) {
            if(Pattern.matches("[0-9,.]+",s)){
                System.out.println(s);
            }

        }

    }

}
