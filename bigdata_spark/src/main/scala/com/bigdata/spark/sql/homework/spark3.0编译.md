1.修改$SPARK_HOME/dev/make-distribution.sh脚本
(a)显式指定版本信息
VERSION=3.0.1
SCALA_VERSION=2.12
SPARK_HADOOP_VERSION=2.6.0-cdh5.16.2
SPARK_HIVE=1

将脚本中原来的检查给注释掉脚本中检查b

(b)增加编译时的内存设置，防止OOM
export MAVEN_OPTS="${MAVEN_OPTS:--Xmx4g -XX:ReservedCodeCacheSize=4g}"

2.修改$SPARK_HOME/pom.xml文件：在repositories标签中新增阿里云的maven仓库地址和cloudera的仓库地址
<repository>
  <id>aliyun</id>
  <name>cloudera Repository</name>
  <url>http://maven.aliyun.com/nexus/content/groups/public/</url>
</repository>

<repository>
  <id>cloudera</id>
  <name>cloudera Repository</name>
  <url>https://repository.cloudera.com/artifactory/cloudera-repos/</url>
</repository>


3.提前下载好zinc,scala之后放在$SPARK_HOME/build
https://downloads.lightbend.com/zinc/0.3.15/zinc-0.3.15.tgz
https://downloads.lightbend.com/scala/2.12.10/scala-2.12.10.tgz


4.编译：
./dev/make-distribution.sh --name 2.6.0-cdh5.16.2 --tgz -Dhadoop.version=2.6.0-cdh5.16.2 -Dscala.version=2.12.10 -Phive-1.2 -Phive-thriftserver -Pyarn
报错deploy\yarn\Client.scala:298: value setRolledLogsIncludePattern is not a member of org.apache.hadoop.yarn.api.records.LogAggregationContext
查到https://github.com/apache/spark/pull/16884/files?w=1
可以打patch或者直接修改
resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/Client.scala 



5.部署测试