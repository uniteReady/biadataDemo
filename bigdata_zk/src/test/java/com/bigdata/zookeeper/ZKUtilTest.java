package com.bigdata.zookeeper;

import com.bigdata.zookeeper.utils.ZKUtil;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ZKUtilTest {
    private static String connect = "bigdata:2181";
    private static int timeout = 99000;
    private static ZooKeeper zk = null;
    private static String path = "/bigdata";
    @Before
    public void setUp(){
         zk = ZKUtil.getZK(connect,timeout);
    }

    @Test
    public void getZKTest(){
        Assert.assertNotNull(zk);

    }

    @Test
    public void createNodeTest(){
        ZKUtil.createNode(zk,path,"123");
    }

    @Test
    public void getDataTest(){
        System.out.println(ZKUtil.getData(zk, path));

    }

    @Test
    public void updateDataTest() throws KeeperException, InterruptedException {
        ZKUtil.updateData(zk,path,"789");
    }



}
