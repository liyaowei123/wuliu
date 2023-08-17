# wuliu  
### 跟着尚硅谷离线物流数仓项目做的  
### 项目架构：  
数据存储：Hadoop、Hive、Mysql；  
数据采集：Flume、Kakfa；  
数据同步：DataX、Flink-cdc;  
数据可视化：Superset。  
  
### 项目流程  
1、产生数据到mysql；  
2、通过flume将数据从mysql采集到kafka；  
3、通过datax将数据从kafka中的数据全量同步到hdfs，flink-cdc增量同步到hdfs，然后使用hive进行分层、建立数仓；  
4、通过datax将hive的ads层的数据同步到mysql，superset进行数据可视化。
