package pers.chris.dbSync.job;

import pers.chris.dbSync.conf.DBConf;
import pers.chris.dbSync.conf.SyncDataConf;
import pers.chris.dbSync.fieldMap.FieldMapManager;
import pers.chris.dbSync.reader.BaseReader;
import pers.chris.dbSync.reader.Reader;
import pers.chris.dbSync.syncData.SyncData;
import pers.chris.dbSync.valueFilter.ValueFilterManager;
import pers.chris.dbSync.writer.BaseWriter;
import pers.chris.dbSync.writer.Writer;
import org.apache.log4j.Logger;

public class Job implements Runnable {

    private String jobId;
    public JobTypeEnum jobType;
    private SyncDataConf syncDataConf;
    private DBConf writerConf;
    private DBConf readerConf;
    private ValueFilterManager valueFilterManager;
    private FieldMapManager fieldMapManager;
    private SyncData syncData;
    private final Logger logger = Logger.getLogger(Job.class);

    @Override
    public void run() {
        syncData = new SyncData();
        Writer writer = BaseWriter.getInstance(jobType, writerConf.dbType);
        Reader reader = BaseReader.getInstance(jobType, readerConf.dbType);

        syncData.setSyncDataConfig(syncDataConf);
        writer.setWriterConfig(writerConf);
        reader.setReaderConfig(readerConf);

        // 监听器注册，数据发生变化时执行方法write
        syncData.registerListener(event -> {
            writer.write(syncData);
        });
        writer.connect();
        reader.connect();

        // 字段信息读取&传入
        writer.readField();
        reader.readField();
        syncData.setWriteFields(writer.getFields());
        syncData.setReadFields(reader.getFields());

        // 数据过滤管理器&字段映射管理器传入
        reader.setValueFilterManager(valueFilterManager);
        syncData.setFieldMapManager(fieldMapManager);

        reader.read(syncData, syncDataConf.getInterval());
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public DBConf getWriterConf() {
        return writerConf;
    }

    public void setWriterConf(DBConf writerConf) {
        this.writerConf = writerConf;
    }

    public DBConf getReaderConf() {
        return readerConf;
    }

    public void setReaderConf(DBConf readerConf) {
        this.readerConf = readerConf;
    }

    public SyncDataConf getSyncDataConf() {
        return syncDataConf;
    }

    public void setSyncDataConf(SyncDataConf syncDataConf) {
        this.syncDataConf = syncDataConf;
    }

    public ValueFilterManager getValueFilterManager() {
        return valueFilterManager;
    }

    public void setValueFilterManager(ValueFilterManager valueFilterManager) {
        this.valueFilterManager = valueFilterManager;
    }

    public FieldMapManager getFieldMapManager() {
        return fieldMapManager;
    }

    public void setFieldMapManager(FieldMapManager fieldMapManager) {
        this.fieldMapManager = fieldMapManager;
    }
}
