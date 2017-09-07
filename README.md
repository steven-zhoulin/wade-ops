wade-ops 运维平台:

* **Crawling**: 负责从生产主机上bomc目录下爬取bomc*.dat文件。
* **Loading**: 负责将爬取过来的文件加载进HBase。

```sh
$ cd ~
$ # Clone the repository to get the source code.
$ git clone https://github.com/steven-zhoulin/wade-ops.git
$ git checkout master
```
#### Build & Run
1. Build the whole sources use the following maven command

```sh
$ cd ~/wade-ops
$ mvn clean install -Dmaven.test.skip
$ mvn compile
$ mvn jar
$ cd ~/bin
$ ./start-crawler.sh
```