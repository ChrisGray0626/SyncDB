package com.chris.reader;

import com.chris.configuration.ReaderConfiguration;
import common.Readable;
import common.Task;

public abstract class AbstractReader extends Task implements Readable {

    private ReaderConfiguration readerConfiguration;

    public ReaderConfiguration getReaderConfig() {
        return readerConfiguration;
    }

    public void setReaderConfig(ReaderConfiguration readerConfiguration) {
        this.readerConfiguration = readerConfiguration;
    }
}