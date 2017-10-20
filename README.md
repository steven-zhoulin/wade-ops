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
rowkey: traceid
    span:web|74138c1248e44ca5b1ac7991b7635711      -> 单个Span的二进制数据(Map结构)
    info:app|74138c1248e44ca5b1ac7991b7635712      -> 单个Span的二进制数据(Map结构) 
    info:service|74138c1248e44ca5b1ac7991b7635713  -> 单个Span的二进制数据(Map结构)
    info:dao|74138c1248e44ca5b1ac7991b7635714      -> 单个Span的二进制数据(Map结构)
    info:browser|74138c1248e44ca5b1ac7991b7635715  -> 单个Span的二进制数据(Map结构)
```

#### trace_menuid
```sql
-- 建表语句:
create 'trace_menuid', 'traceid'
alter  'trace_menuid', {NAME => 'traceid', TTL => '259200'}
```

```sql
-- 表示例:
rowkey: menuid|时间戳 [每10分钟一个段]
    traceid: traceid1
    traceid: traceid2
    traceid: traceid3
```

#### trace_sn
```sql
-- 建表语句:
create 'trace_sn', 'traceid'
alter  'trace_sn', {NAME => 'traceid', TTL => '259200'}
```

```sql
-- 表示例:
rowkey: sn|时间戳 [每10分钟一个段]
    traceid: traceid1
    traceid: traceid2
    traceid: traceid3
```