package com.bigdata.zookeeper.utils;

import org.apache.zookeeper.*;

import java.io.IOException;

public class ZKUtil {

    public static ZooKeeper getZK(String connnet,int timeout){

        try {
            return new ZooKeeper(connnet, timeout, new Watcher() {
                @Override
                public void process(WatchedEvent event) {

                }
            });
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void createNode(ZooKeeper zk,String path,String value){

        try {
            //第三个参数是权限
            zk.create(path,value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public static String getData(ZooKeeper zk,String path){
        try {
            return new String(zk.getData(path,false,null)) ;
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    public static void updateData(ZooKeeper zk,String path,String value) throws KeeperException, InterruptedException {

            zk.setData(path,value.getBytes(),-1);

    }




}
