package com.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;



public class HBaseMultiVersion {
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

    /**
     * 多版本
     * @param tableName
     */
    public static void getAllRecord(String tableName,Integer version) {

        try {
            Table table=connection.getTable(TableName.valueOf(tableName));
            Scan scan=new Scan();
            //默认为1 ，可设置值为3版本(最新的3个版本)，将历史数据打印
            scan.setMaxVersions(version);

            ResultScanner rs=table.getScanner(scan);
            for (Result r:rs) {
                for (Cell cell : r.rawCells()) {
                    System.out.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
                            +" : "+Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength())
                            +":"+Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength())
                            +" : "+Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        getAllRecord("bigdata:pc",2);
    }
}
