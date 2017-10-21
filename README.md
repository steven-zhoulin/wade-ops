wade-ops 运维平台:

* **Crawling**: 负责从生产主机上bomc目录下爬取bomc*.dat文件。
* **Loading**: 负责将爬取过来的文件加载进HBase。

```sh
$ cd ~
$ # Clone the repository to get the source code.
$ git clone https://github.com/steven-zhoulin/wade-ops.git
$ git checkout master
```
### Build & Run
1. Build the whole sources use the following maven command

```sh
$ cd ~/wade-ops
$ mvn clean install -Dmaven.test.skip
$ mvn compile
$ mvn jar
$ cd ~/bin
$ ./start-crawler.sh
```

### 表模型设计

#### trace

```sql
-- 建表语句:
create 'trace', 'span'
alter  'trace', {NAME => 'span', TTL => '259200'}
```
```sql
-- 表结构示例:    
put 'trace', 'web-74138c1248e44ca5b1ac7991b7635711', 'span:browser|74138c1248e44ca5b1ac7991b7635711', byte[]
put 'trace', 'web-74138c1248e44ca5b1ac7991b7635711', 'span:web|74138c1248e44ca5b1ac7991b7635712', byte[]
put 'trace', 'web-74138c1248e44ca5b1ac7991b7635711', 'span:app|74138c1248e44ca5b1ac7991b7635713', byte[]
put 'trace', 'web-74138c1248e44ca5b1ac7991b7635711', 'span:service|74138c1248e44ca5b1ac7991b7635714', byte[]
put 'trace', 'web-74138c1248e44ca5b1ac7991b7635711', 'span:dao|74138c1248e44ca5b1ac7991b7635715', byte[]
put 'trace', 'web-74138c1248e44ca5b1ac7991b7635711', 'span:dao|74138c1248e44ca5b1ac7991b7635716', byte[]
```

#### trace_menu (需要准实时入HBase, menuid只有prototype=web才有, 难度系数:低)
```sql
-- 建表语句:
create 'trace_menu', 'tid'
alter  'trace_menu', {NAME => 'tid', TTL => '259200'}
```

```sql
-- 表示例: 
        表名                  rowkey                        colname                   value
put 'trace_menu', 'CRM0001^201710201430', 'tid:web-74138c1248e44ca5b1ac7991b7635711', ''
put 'trace_menu', 'CRM0001^201710201430', 'tid:web-74138c1248e44ca5b1ac7991b7635712', ''
put 'trace_menu', 'CRM0001^201710201430', 'tid:web-74138c1248e44ca5b1ac7991b7635713', ''
put 'trace_menu', 'CRM0001^201710201430', 'tid:web-74138c1248e44ca5b1ac7991b7635714', ''
put 'trace_menu', 'CRM0001^201710201430', 'tid:web-74138c1248e44ca5b1ac7991b7635715', ''
```

#### trace_sn (需要准实时入HBase, 采用: 13007318123^201710201430 -> web-74138c1248e44ca5b1ac7991b7635711, 难度系数: 一般，整个时间片入完后再一次性插入！)
```sql
-- 建表语句:
create 'trace_sn', 'tid'
alter  'trace_sn', {NAME => 'tid', TTL => '259200'}
```

```sql
-- 表示例: 
        表名                  rowkey                           colname                 value
put 'trace_sn', '13007318123^201710201430', 'tid:web-74138c1248e44ca5b1ac7991b7635711', ''
put 'trace_sn', '13007318123^201710201430', 'tid:web-74138c1248e44ca5b1ac7991b7635712', ''
put 'trace_sn', '13007318123^201710201430', 'tid:web-74138c1248e44ca5b1ac7991b7635713', ''
put 'trace_sn', '13007318123^201710201430', 'tid:web-74138c1248e44ca5b1ac7991b7635714', ''
put 'trace_sn', '13007318123^201710201430', 'tid:web-74138c1248e44ca5b1ac7991b7635715', ''
```

#### trace_operid (需要准实时入HBase，采用: )
```sql
-- 建表语句:
create 'trace_operid', 'tid'
alter  'trace_operid', {NAME => 'tid', TTL => '259200'}
```

```sql
-- 表示例: 
        表名                  rowkey                           colname                 value
put 'trace_operid', 'SUPERUSR^201710201430', 'tid:web-74138c1248e44ca5b1ac7991b7635711', ''
put 'trace_operid', 'SUPERUSR^201710201430', 'tid:web-74138c1248e44ca5b1ac7991b7635712', ''
put 'trace_operid', 'SUPERUSR^201710201430', 'tid:web-74138c1248e44ca5b1ac7991b7635713', ''
put 'trace_operid', 'SUPERUSR^201710201430', 'tid:web-74138c1248e44ca5b1ac7991b7635714', ''
put 'trace_operid', 'SUPERUSR^201710201430', 'tid:web-74138c1248e44ca5b1ac7991b7635715', ''
```

#### trace_service
```sql
-- 建表语句：
create 'trace_service', 'tid'
alter  'trace_service', {NAME => 'tid', TTL => '259200'}
```

```sql
-- 表示例: 
        表名                  rowkey                           colname                   value
put 'trace_service', 'SVCNAME1^201710201430', 'tid:web-74138c1248e44ca5b1ac7991b7635711', ''
put 'trace_service', 'SVCNAME1^201710201430', 'tid:web-74138c1248e44ca5b1ac7991b7635712', ''
put 'trace_service', 'SVCNAME1^201710201430', 'tid:web-74138c1248e44ca5b1ac7991b7635713', ''
put 'trace_service', 'SVCNAME1^201710201430', 'tid:web-74138c1248e44ca5b1ac7991b7635714', ''
put 'trace_service', 'SVCNAME1^201710201430', 'tid:web-74138c1248e44ca5b1ac7991b7635715', ''
```


#### service_map 服务地图
```sql
-- 建表语句:
create 'service_map', 'relat'
alter  'service_map', {NAME => 'direct', TTL => '259200'}
```

```sql
-- 示例:
        表名          rowkey                   colname            value
put 'service_map', 'servicename0^20171020', 'relat:servicename1', ''
put 'service_map', 'servicename0^20171020', 'relat:servicename1', ''
put 'service_map', 'servicename0^20171020', 'relat:servicename1', ''
put 'service_map', 'servicename0^20171020', 'relat:servicename1', ''
put 'service_map', 'servicename0^20171020', 'relat:servicename1', ''
```