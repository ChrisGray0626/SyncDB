package pers.chris.dbSync.job;

import pers.chris.dbSync.conf.DBConf;
import pers.chris.dbSync.conf.SyncDataConf;
import pers.chris.dbSync.fieldMapper.FieldMapManager;
import pers.chris.dbSync.reader.Reader;
import pers.chris.dbSync.syncData.SyncData;
import pers.chris.dbSync.util.ClassLoadUtil;
import pers.chris.dbSync.valueFilter.ValueFilterManager;
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
    private Writer writer;
    private Reader reader;
    private final Logger logger = Logger.getLogger(Job.class);

    @Override
    public void run() {
        syncData = new SyncData();

        try {
            switch (jobType) {
                case PUSH:
                    reader = ClassLoadUtil.loadReaderClass(readerConf.dbType);
                    break;
                case PULL:
                    reader = new Reader();
                    break;
                default:
            }
            writer = ClassLoadUtil.loadWriterClass(writerConf.dbType);
        }
        catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            logger.error(e);
        }

        syncData.setSyncDataConfig(syncDataConf);
        writer.setWriterConfig(writerConf);
        reader.setReaderConfig(readerConf);

        syncData.registerListener(event -> {
            writer.write(syncData);
        });
        writer.connect();
        reader.connect();

        // 读取字段信息
        writer.readField();
        reader.readField();

        // 配置&加载数据过滤管理器
        valueFilterManager.config();
        reader.setValueFilterManager(valueFilterManager);

        // 配置&检查&加载字段映射管理器
        fieldMapManager.config();
        fieldMapManager.check(writer.getFieldNames(), reader.getFieldNames());
        syncData.setFieldMapManager(fieldMapManager);
        reader.read(syncData, syncDataConf.getInterval());
    }

    private void runByPull() {

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
