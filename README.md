# rabbitmq-c-libevent

Use rabbitmq-c based on libevent.
提供基于 libevent 改造的 rabbitmq-c 版本。


感谢 alanxz 实现的 [alanxz/rabbitmq-c](https://github.com/alanxz/rabbitmq-c)
rabbitmq-c-libevent 代码是基于 rabbitmq-c-0.4.1 版本进行的功能添加。

### Features:

- 可以通过 API 方便的添加 producer 、consumer ，以及 mananger 等角色进行相关业务处理；
- 基于 event-driven 模式实现，故能够在单线程中同时处理任意多个 producer 和 consumer ；

### TODO:

- 添加缺失的 AMQP 功能特性