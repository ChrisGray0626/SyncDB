# SyncDB

## Reader

### MySQLReader

#### 二进制日志（binlog）

1. 在my.ini中配置开启binLog服务

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

1. 使用开源框架[mysql-binlog-connector-java]([shyiko/mysql-binlog-connector-java: MySQL Binary Log connector (github.com)](https://github.com/shyiko/mysql-binlog-connector-java))监听binlog

#### 数据格式

- TableMapEventData：包含变化的库名和表名
- WriteRowsEventData：包含新增数据内容（不含字段名），默认格式为List<Serializable[]>
- 不同事件类型对应EventData的不同EventType

### PostgreSQLReader

#### 逻辑解码（Logical Decoding）

1. 配置wal_level = logical

1. 创建逻辑复制槽（Replication Slot），使用解码插件 （当前使用test_decoding）

   ```sql
   SELECT * FROM pg_create_logical_replication_slot('test_slot', 'test_decoding');
   ```

1. 从逻辑复制槽中获取变更数据

   ```sql
   -- 查看并消费
   pg_logical_slot_get_changes(slot_name name, ...)
   -- 只查看不消费
   pg_logical_slot_peek_changes(slot_name name, ...)
   ```

1. 定时轮询逻辑复制槽

#### 数据格式

- table：包含模式、表名、事件类型、数据内容（含字段名与字段类型），且均在一个字段data中
- 格式Text的数据自带单引号

#### 注意事项

- 如果修改逻辑复制槽名称，请使用PostgreSQLReader的方法setLogicalReplicationSlotName设置名称。

### SQLServerReader

#### 变更数据捕获（Change Data Capture）

1. 启动 SQL Server 代理服务
1. 启动数据库CDC服务

    ```sql
    USE 'DBName'
    GO
    EXECUTE sys.sp_cdc_enable_db;
    GO
    -- 检查是否成功
    SELECT is_cdc_enabled FROM sys.databases WHERE NAME = 'DBName'
    ```

1. 启动库表CDC服务

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

    -- 检查是否成功
    SELECT name, is_tracked_by_cdc FROM sys.tables WHERE OBJECT_ID= OBJECT_ID('dbo.tableName')

    -- 禁用
    EXEC sys.sp_cdc_disable_table  
    @source_schema = N'dbo',  
    @source_name   = N'tableName',  
    @capture_instance = N'dbo_tableName'  
    ```

1. 查看捕获数据

    ```sql
    SELECT * FROM cdc.dbo_tableName_CT
    
    -- 查询间隔时间interval（分钟）内的捕获数据
    DECLARE @bglsn VARBINARY(10)=sys.fn_cdc_map_time_to_lsn('smallest greater than or equal',DATEADD(mi,-delayTime, GETDATE()));
    DECLARE @edlsn VARBINARY(10)=sys.fn_cdc_map_time_to_lsn('largest less than or equal',GETDATE());
    SELECT * FROM cdc.dbo_ + tableName + _CT WHERE [__$start_lsn] BETWEEN @bglsn AND @edlsn);
    ```

1. 定时轮询CDC库表

### 数据格式

- dbo_tableName_CT：包含表名、事件类型、数据内容（含字段名）
- 表名直接为CDC表名的一部分
- 事件类型由字段__$operation表示
- 数据字段直接为CDC表字段的一部分

#### 注意事项

- 如果库表有字段更新需要禁用并重新启动CDC服务，否则将无法记录新增字段并且出错。

- 因为JDK版本不推荐使用旧的TLSV1.0的协议，所以默认删除TLS10的支持。

    > The server selected protocol version TLS10 is not accepted by client preferences [TLS12]"

    jre\lib\security文件夹下，编辑java.security文件，其中找到 jdk.tls.disabledAlgorithms配置项，将TLSv1, TLSv1.1, 3DES_EDE_CBC删除。

- CDC文件中字段内容末尾默认有大量空格。

## Writer

### 监听同步数据集

1. 注册监听器（registerListener）
1. 重写监听器的方法doSet
1. 监听同步数据集的数据变化

### PostgreSQLWriter

## 同步数据集（SyncData）

### 监听器（SyncDataListener）

监听器建立在属性rows上，每次调用方法setRows后，将调用监听器的方法doSet。

### 数据格式

List<String> rows

## 数据映射（Mapper）

## 数据过滤（Filter）

## 配置信息（Config）

配置信息的具体内容存储至数据库中。

### 配置连接（MySQL）

#### 文件位置

resources/conf.properties

#### 配置信息

```sql
configuration.hostname = localhost
configuration.port = 3306
configuration.databaseName = test
configuration.user = root
configuration.password = 123456
configuration.tableName = db_sync_config
```

### 配置获取



### 配置规范

#### DBType

- MYSQL
- POSTGRESQL
- SQLSERVER

## 日志（Log）

### 存储方式（MySQL）

```sql
create table log(
	id int(4) PRIMARY KEY AUTO_INCREMENT,
	create_time datetime,
	logs VARCHAR(255)
)
```

### log4j配置

```properties
log4j.appender.Database=org.apache.log4j.jdbc.JDBCAppender
log4j.appender.Database.driver=com.mysql.cj.jdbc.Driver
log4j.appender.Database.URL=jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8
log4j.appender.Database.user=root
log4j.appender.Database.password=123456
log4j.appender.Database.sql=insert into log (create_time, log) VALUES ("%d{yyyy-MM-dd hh:mm:ss}", "%c %m%n")
log4j.appender.Database.Threshold = ERROR
log4j.appender.Database.layout=org.apache.log4j.PatternLayout
```

## 注意事项

- 抽象类Writer/Reader必须与实现类放在同一package下，否则无法动态加载实现类。

