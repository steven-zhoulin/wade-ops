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
$ ./start-load.sh
$ ./start-analyze.sh
```

### 表模型设计

#### trace

```html
-- 建表语句:
create 'trace', 'span'
alter  'trace', {NAME => 'span', TTL => '15552000'}
```
```html
-- 表结构示例:    
put 'trace', 'web-74138c1248e44ca5b1ac7991b7635711', 'span:browser|74138c1248e44ca5b1ac7991b7635711', byte[]
put 'trace', 'web-74138c1248e44ca5b1ac7991b7635711', 'span:web|74138c1248e44ca5b1ac7991b7635712', byte[]
put 'trace', 'web-74138c1248e44ca5b1ac7991b7635711', 'span:app|74138c1248e44ca5b1ac7991b7635713', byte[]
put 'trace', 'web-74138c1248e44ca5b1ac7991b7635711', 'span:service|74138c1248e44ca5b1ac7991b7635714', byte[]
put 'trace', 'web-74138c1248e44ca5b1ac7991b7635711', 'span:dao|74138c1248e44ca5b1ac7991b7635715', byte[]
put 'trace', 'web-74138c1248e44ca5b1ac7991b7635711', 'span:dao|74138c1248e44ca5b1ac7991b7635716', byte[]
```

#### trace_menu (需要准实时入HBase, menuid只有prototype=web才有, 难度系数:低)
```html
-- 建表语句:
create 'trace_menu', 'info'
alter  'trace_menu', {NAME => 'info', TTL => '15552000'}
```

```html
-- 表示例: 
        表名               rowkey                       info:tid                 
put 'trace_menu', 'CRM0001^1508729999000', 'web-74138c1248e44ca5b1ac7991b7635711'
put 'trace_menu', 'CRM0001^1508729999001', 'web-74138c1248e44ca5b1ac7991b7635712'
put 'trace_menu', 'CRM0001^1508729999002', 'web-74138c1248e44ca5b1ac7991b7635713'
put 'trace_menu', 'CRM0001^1508729999003', 'web-74138c1248e44ca5b1ac7991b7635714'
put 'trace_menu', 'CRM0001^1508729999004', 'web-74138c1248e44ca5b1ac7991b7635715'
```

#### trace_operid (需要准实时入HBase，采用: )
```html
-- 建表语句:
create 'trace_operid', 'info'
alter  'trace_operid', {NAME => 'info', TTL => '15552000'}
```

```html
-- 表示例: 
        表名                  rowkey                       info:tid
put 'trace_operid', 'SUPERUSR^201710201430', 'web-74138c1248e44ca5b1ac7991b7635711'
put 'trace_operid', 'SUPERUSR^201710201430', 'web-74138c1248e44ca5b1ac7991b7635712'
put 'trace_operid', 'SUPERUSR^201710201430', 'web-74138c1248e44ca5b1ac7991b7635713'
put 'trace_operid', 'SUPERUSR^201710201430', 'web-74138c1248e44ca5b1ac7991b7635714'
put 'trace_operid', 'SUPERUSR^201710201430', 'web-74138c1248e44ca5b1ac7991b7635715'
```

#### trace_sn (需要准实时入HBase, 采用: 13007318123^201710201430 -> web-74138c1248e44ca5b1ac7991b7635711, 难度系数: 一般，整个时间片入完后再一次性插入！)
```html
-- 建表语句:
create 'trace_sn', 'info'
alter  'trace_sn', {NAME => 'info', TTL => '15552000'}
```

```html
-- 表示例: 
        表名                  rowkey                       info:tid                 
put 'trace_sn', '13007318123^201710201430', 'web-74138c1248e44ca5b1ac7991b7635711'
put 'trace_sn', '13007318123^201710201430', 'web-74138c1248e44ca5b1ac7991b7635712'
put 'trace_sn', '13007318123^201710201430', 'web-74138c1248e44ca5b1ac7991b7635713'
put 'trace_sn', '13007318123^201710201430', 'web-74138c1248e44ca5b1ac7991b7635714'
put 'trace_sn', '13007318123^201710201430', 'web-74138c1248e44ca5b1ac7991b7635715'
```

#### trace_service
```html
-- 建表语句：
create 'trace_service', 'info'
alter  'trace_service', {NAME => 'info', TTL => '15552000'}
```

```html
-- 表示例: 
        表名                  rowkey                         info:tid
put 'trace_service', 'SVCNAME1^201710201430', 'web-74138c1248e44ca5b1ac7991b7635711'
put 'trace_service', 'SVCNAME1^201710201430', 'web-74138c1248e44ca5b1ac7991b7635712'
put 'trace_service', 'SVCNAME1^201710201430', 'web-74138c1248e44ca5b1ac7991b7635713'
put 'trace_service', 'SVCNAME1^201710201430', 'web-74138c1248e44ca5b1ac7991b7635714'
put 'trace_service', 'SVCNAME1^201710201430', 'web-74138c1248e44ca5b1ac7991b7635715'
```

#### service_map_menu 服务菜单地图，从服务依赖关系，找到对应的菜单。
```html
create 'sink_service_relat', 'dependService', 'beDependService', 'beDependMenuId'
alter  'sink_service_relat', {NAME => 'dependService', TTL => '259200'}
alter  'sink_service_relat', {NAME => 'beDependService', TTL => '259200'}
alter  'sink_service_relat', {NAME => 'beDependMenuId', TTL => '259200'}
```