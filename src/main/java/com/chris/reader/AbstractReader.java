package com.chris.reader;

import com.chris.syncData.SyncData;

public abstract class AbstractReader {

    // TODO 库表筛选
    // 配置数据库信息
    public abstract void config(String fileName);
    // 初始化同步数据集
    public abstract void init(SyncData syncData);
    public abstract void connect();
    public abstract void read();
    public abstract void close();
}