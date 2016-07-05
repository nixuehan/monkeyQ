MonkeyQ-队列服务
============

以redis作为持久存储. golang实现的 消息队列。 支持 延迟队列 多实例等。项目急要，刚写完~~ 边上线边填坑吧。。

monkeyQTools  是一个管理队列的小工具~~  主要方便我临时管理下队列。

运行即可


    $ ./monkeyQ -host 8.8.8.8 -port 9394 -redis 127.0.0.1:6379 -auth 2016


###支持的参数：

Usage of ./monkeyQ:

  -host string

    	Bound IP. default:localhost (default "localhost")

  -port string

    	port. default:9394 (default "9394")

  -redis string

    	redis server. default:127.0.0.1:6379 (default "127.0.0.1:6379")

  -auth string

    	redis server auth password
