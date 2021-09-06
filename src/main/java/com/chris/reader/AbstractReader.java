package com.chris.reader;

import com.chris.config.ReaderConfig;
import common.Readable;
import common.Task;

public abstract class AbstractReader extends Task implements Readable {

    public ReaderTypeEnum readerType;
    private ReaderConfig readerConfig;

    public abstract void config(String fileName);

    public ReaderConfig getReaderConfig() {
        return readerConfig;
    }

    public void setReaderConfig(ReaderConfig readerConfig) {
        this.readerConfig = readerConfig;
    }
}