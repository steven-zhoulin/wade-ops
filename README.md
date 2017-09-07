wade-ops 运维平台.

包括两个方面:

* **Crawling**: 负责从生产主机上bomc目录下爬取bomc*.dat文件。
* **Loading**: 负责将爬取过来的文件加载进HBase。

For more details, please refer to [dubbo.io](http://dubbo.io).

## Documentation

* [User's Guide](http://dubbo.io/user-guide/)
* [Developer's Guide](http://dubbo.io/developer-guide/)
* [Admin's Guide](http://dubbo.io/admin-guide/)

## Quick Start
This guide gets you started with dubbo with a simple working example.
#### Download the sources(examples)
You’ll need a local copy of the example code to work through this quickstart. Download the demo code from our [Github repository](https://github.com/alibaba/dubbo) (the following command clones the entire repository, but you just need the `dubbo-demo` for this quickstart and other tutorials):

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
