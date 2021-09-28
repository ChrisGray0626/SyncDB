package pers.chris.dbSync.job;

import pers.chris.dbSync.common.typeEnum.JobTypeEnum;
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

import java.util.concurrent.TimeUnit;

public class Job implements Runnable {

    private String jobId;
    public JobTypeEnum jobType;
    private String dstDBConfId;
    private String srcDBConfId;
    private DBConf dstDBConf;
    private DBConf srcDBConf;
    private SyncDataConf syncDataConf;
    private ValueFilterManager valueFilterManager;
    private FieldMapManager fieldMapManager;
    private SyncData syncData;
    private Reader reader;
    private Writer writer;
    private final Logger logger = Logger.getLogger(Job.class);

    @Override
    public void run() {
        // Job信息打印
        console();

        syncData = new SyncData();
        reader = BaseReader.getInstance(jobType, srcDBConf.dbType);
        writer = BaseWriter.getInstance(jobType, dstDBConf.dbType);

        syncData.setSyncDataConfig(syncDataConf);
        reader.setReaderConfig(srcDBConf);
        writer.setWriterConfig(dstDBConf);

        reader.connect();
        writer.connect();

        // 字段信息读取&传入
        reader.readField();
        writer.readField();
        syncData.setReadFields(reader.getFields());
        syncData.setWriteFields(writer.getFields());

        // 数据过滤管理器传入，数据过滤在读取时完成
        reader.setValueFilterManager(valueFilterManager);

        // 监听器注册，数据发生变化时执行后续流程
        syncData.registerListener(event -> {
            fieldMapManager.run(syncData);
            writer.write(syncData);
        });

        switch (jobType) {
            case TIMED:
                runTimed();
                break;
            case REAL:
                runReal();
                break;
            default:
        }
    }

    public void runTimed() {
        int interval = syncDataConf.getInterval();

        while (true) {
            reader.read(syncData, interval);
            try {
                TimeUnit.MINUTES.sleep(interval);
            }
            catch (InterruptedException e) {
                logger.error(e);
            }
        }
    }

    public void runReal() {
        reader.read(syncData, syncDataConf.getInterval());
    }

    public void console() {
        String jobInfo = "Job {"
                + "DstDBType=" + dstDBConf.dbType
                + ", SrcDBType=" + srcDBConf.dbType
                + "}";
        logger.debug(jobInfo);
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getDstDBConfId() {
        return dstDBConfId;
    }

    public void setDstDBConfId(String dstDBConfId) {
        this.dstDBConfId = dstDBConfId;
    }

    public String getSrcDBConfId() {
        return srcDBConfId;
    }

    public void setSrcDBConfId(String srcDBConfId) {
        this.srcDBConfId = srcDBConfId;
    }

    public DBConf getDstDBConf() {
        return dstDBConf;
    }

    public void setDstDBConf(DBConf dstDBConf) {
        this.dstDBConf = dstDBConf;
    }

    public DBConf getSrcDBConf() {
        return srcDBConf;
    }

    public void setSrcDBConf(DBConf srcDBConf) {
        this.srcDBConf = srcDBConf;
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
