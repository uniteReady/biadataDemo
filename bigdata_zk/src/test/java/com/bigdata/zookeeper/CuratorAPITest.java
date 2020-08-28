package com.bigdata.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CuratorAPITest {

    CuratorFramework client = null;
    String zkQuorum = "bigdata:2181";
    String nodePath = "/curator/bigdata";

    @Before
    public void setup(){
        RetryPolicy retry = new ExponentialBackoffRetry(1000, 5);

        client = CuratorFrameworkFactory.builder()
                .connectString(zkQuorum)
                .sessionTimeoutMs(10000)
                .retryPolicy(retry)
                .namespace("bigdata_namespace")
                .build();

        client.start();
    }

    @Test
    public void createTest() throws Exception {
        client.create().creatingParentContainersIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath(nodePath + "/a/b","hello".getBytes());

    }

    @Test
    public void setDataTest() throws Exception {
        client.setData()
                .forPath(nodePath,"goodbye".getBytes());
    }


    @Test
    public void getDataVersionTest() throws Exception {
        Stat stat = new Stat();
        String data = new String(client.getData().storingStatIn(stat).forPath(nodePath));
        System.out.println(stat.getAversion());
        System.out.println(data);

    }


    @Test
    public void ifExistsTest() throws Exception {
//        Stat stat = client.checkExists().forPath(nodePath);
        Stat stat = client.checkExists().forPath("/test");
        System.out.println(stat);

    }


    @Test
    public void deleteTest() throws Exception {
        client.delete()
                .deletingChildrenIfNeeded()
                .forPath(nodePath + "/a");

    }





    @After
    public void tearDown(){

        if(null != client){
            client.close();
        }
    }
}
