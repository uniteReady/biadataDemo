package com.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseCURD {
        public static Configuration conf;
        public static Connection connection;

        static {
            Configuration HBASE_CONFIG = new Configuration();
            HBASE_CONFIG.set("hbase.zookeeper.Quorum","bigdata");
            HBASE_CONFIG.set("hbase.zookeeper.property.clientPort","2181");
            // 解决No FileSystem for scheme: hdfs错误
            HBASE_CONFIG.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            conf = HBaseConfiguration.create(HBASE_CONFIG);

            try {
                 connection = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }


        }


    public static void main(String[] args) {
            String[] arr = {"computer","laptop"};

//            HbaseCURD.createTable("bigdata:pc",arr);
//            HbaseCURD.putRecord("bigdata:pc","row2","laptop","lenovo","xiaoxin16");
//            HbaseCURD.getRecord("bigdata:pc","row1");
//            HbaseCURD.getAllRecord("bigdata:pc");
            HbaseCURD.deleteRecords("bigdata:pc","row1");
    }


        public static void createTable(String tableName,String[] families){
            try {
                Admin admin = connection.getAdmin();
                if(admin.tableExists(TableName.valueOf(tableName))){
                    System.out.println("table already exists!");
                }else {
                    HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
                    for (String family : families) {
                        hTableDescriptor.addFamily(new HColumnDescriptor(family));
                    }

                    admin.createTable(hTableDescriptor);
                    System.out.println("create Table");
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

        }


        public static void putRecord(String tableName,String rowKey,String family,String qualifier,String value){
            try {
                Table table = connection.getTable(TableName.valueOf(tableName));
                Put put = new Put(rowKey.getBytes());
                put.addColumn(family.getBytes(),qualifier.getBytes(),value.getBytes());
                table.put(put);

                System.out.println("finish putRecord");

            } catch (IOException e) {
                e.printStackTrace();
            }

        }


        public static void getRecord(String tableName,String row){
            try {
                Table table = connection.getTable(TableName.valueOf(tableName));
                Get get = new Get(row.getBytes());
                Result rs = table.get(get);

                for(Cell cell :rs.rawCells()){

                    System.out.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
                            +" : "+Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength())
                            +":"+Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength())
                            +" : "+Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }

            } catch (IOException e) {
                e.printStackTrace();
            }


        }





    public static void getAllRecord(String tableName) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);

            for (Result rs : scanner) {
                for (Cell cell : rs.rawCells()) {

                    System.out.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
                            + " : " + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
                            + ":" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
                            + " : " + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
            }


        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    /**
     * 使用代码删除会直接将所有版本都删除，而shell窗口只会删除最新的版本
     * @param tableName
     * @param rowKey
     */


    public static void deleteRecords(String tableName,String rowKey){
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            List<Delete> list = new ArrayList<>();
            Delete delete = new Delete(rowKey.getBytes());
            list.add(delete);
            table.delete(list);

            System.out.println("finish delete");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }












}
