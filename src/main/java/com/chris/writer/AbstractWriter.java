package com.chris.writer;

import com.chris.config.WriterConfig;
import common.Task;
import common.Writeable;


public abstract class AbstractWriter extends Task implements Writeable {

    public WriterTypeEnum writerType;
    private WriterConfig writerConfig;

    public abstract void config(String fileName);

    public WriterConfig getWriterConfig() {
        return writerConfig;
    }

    public void setWriterConfig(WriterConfig writerConfig) {
        this.writerConfig = writerConfig;
    }
}
