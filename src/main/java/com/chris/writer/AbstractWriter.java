package com.chris.writer;

import com.chris.syncData.SyncData;

public abstract class AbstractWriter {

    // 配置数据库信息
    public abstract void config(String fileName);
    // 初始化同步数据集
    public abstract void init(SyncData syncData);
    public abstract void connect();
    public abstract void write();
    public abstract void close();
}
