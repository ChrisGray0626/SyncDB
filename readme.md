# SyncDB

## Reader

// TODO 不同监听机制梳理

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

### SQLServerReader

#### 启动 SQL Server 代理服务

#### 启动指定数据库CDC服务

```sql
USE 'DBName'
GO
EXECUTE sys.sp_cdc_enable_db;
GO
-- 检查是否成功
SELECT is_cdc_enabled FROM sys.databases WHERE NAME = 'DBName'
```

#### 启动指定库表CDC服务

```sql
EXEC sys.sp_cdc_enable_table 
    @source_schema= 'dbo',
       @source_name = 'tableName',
       @role_name = N'cdc_Admin',
       @capture_instance = DEFAULT, -- 新建cdc表名称
       @supports_net_changes = 1,
    @index_name = NULL,
    @captured_column_list = NULL, -- 需要捕获的字段列表
    @filegroup_name = DEFAULT

--检查是否成功
SELECT name, is_tracked_by_cdc FROM sys.tables WHERE OBJECT_ID= OBJECT_ID('dbo.tableName')

-- 禁用
EXEC sys.sp_cdc_disable_table  
@source_schema = N'dbo',  
@source_name   = N'tableName',  
@capture_instance = N'dbo_tableName'  
```

#### 查看捕获数据

```sql
SELECT * FROM cdc.dbo_tableName_CT
```

**如果库表有字段更新需要禁用并重新启动CDC服务，否则将无法记录新增字段并且出错。**

**因为JDK版本不推荐使用旧的TLSV1.0的协议，所以默认删除TLS10的支持。**

> The server selected protocol version TLS10 is not accepted by client preferences [TLS12]"

根据环境变量配置中jre的地址，在 jre\lib\security文件夹下，编辑java.security文件
在文件中找到 jdk.tls.disabledAlgorithms配置项，将TLSv1, TLSv1.1, 3DES_EDE_CBC删除即可。

CDC文件中字段内容末尾有大量空格。

## Writer

### write()

注册SyncData的监听器，以监听其中的数据变更。

## 同步数据集（SyncData）

## 配置文件

文件位置：resources/conf.properties

## 注意事项

1. 当前Writer的初始化必须先于Reader，因为当前对于同步数据集SyncData的监听设置是在Writer的write()方法中完成的。
2. 当前要求同步双方库表同名。

