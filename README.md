# wuliu
个人物流数仓项目
项目总体流程：
1.数据采集；
2.数仓模型搭建；
3.数据可视化。

项目用到的框架有：Hadoop-3.3.4，Hive-3.1.3，kafka_2.12-3.3.1，Flume-1.10.1，Mysql-8.0.31，Flink-CDC，Datax，SuperSet等。
                    
项目总体流程：模拟生成数据 ——> Mysql ——> 数据同步 ——> HDFS ——> 数仓建模 ——> 数据分析 ——> 可视化。
数据同步分为增量同步和全量同步，全量同步使用datax直接将数据同步到hdfs；增量同步使用flink-cdc将MySQL数据读取到kafka,然后flume将数据从kafka中采集到hdfs。
