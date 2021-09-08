package com.chris.writer;

import com.chris.configuration.WriterConfiguration;
import common.Task;
import common.Writeable;


public abstract class AbstractWriter extends Task implements Writeable {

    private WriterConfiguration writerConfiguration;

    public WriterConfiguration getWriterConfig() {
        return writerConfiguration;
    }

    public void setWriterConfig(WriterConfiguration writerConfiguration) {
        this.writerConfiguration = writerConfiguration;
    }
}
