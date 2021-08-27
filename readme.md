# SyncDB

## Reader

### MySQLReader

#### 监听binLog

1. 使用开源框架[mysql-binlog-connector-java]([shyiko/mysql-binlog-connector-java: MySQL Binary Log connector (github.com)](https://github.com/shyiko/mysql-binlog-connector-java))
2. 在my.ini中配置开启binlog

```bash
#开启binlog日志
log_bin=ON
#binlog日志的基本文件名
log_bin_basename=/var/lib/mysql/mysql-bin
#binlog文件的索引文件，管理所有binlog文件
log_bin_index=/var/lib/mysql/mysql-bin.index
#配置serverid
server-id=1
```

### PostgreSQLReader

#### 逻辑解码（Logical Decoding）

1. 配置wal_level = logical

2. 创建逻辑复制槽（Replication Slot），使用解码插件 （如test_decoding）

   ```sql
   SELECT * FROM pg_create_logical_replication_slot('test_slot', 'test_decoding');
   ```

3. 从逻辑复制槽中获取变更数据

   ```sql
   -- 查看并消费
   pg_logical_slot_get_changes(slot_name name, ...)
   -- 只查看不消费
   pg_logical_slot_peek_changes(slot_name name, ...)
   ```

## Writer

### write()

注册SyncData的监听器，以监听其中的数据变更。

## 同步数据集（SyncData）

## 配置文件

文件位置：resources/conf.properties

## 注意事项

1. Writer的初始化必须先于Reader，因为当前对于同步数据集SyncData的监听设置是在Writer的write()方法中完成的。
