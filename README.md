# mysql binary logs

## MySQL Binlog 8种应用场景

### 数据复制

### 数据修复

### 数据回滚

### 数据审查

### 数据平滑迁移

同步、双写、校验、切读、切写

### 数据缓存，整合 Redis

```shell
$ wget https://download.redis.io/redis-stable.tar.gz
$ tar -zxvf redis-stable.tar.gz 
$ cd redis-stable/
$ make
$ make install
$ vim Makefile 
$ vim src/Makefile
$ cd redis/
$ cd redis-stable/
$ make install PREFIX=/usr/local/redis
$ cd bin/
$ ./redis-server 
$ ./redis-server &
$ ps -ef | grep redis
$ ./redis-cli
```

### 离线处理，整合 RocketMQ

### 外部索引，整合 Elasticsearch