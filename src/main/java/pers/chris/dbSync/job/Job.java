package pers.chris.dbSync.job;

import pers.chris.dbSync.conf.DBConf;
import pers.chris.dbSync.conf.JobConf;
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
    private JobConf jobConf;
    private DBConf dstDBConf;
    private DBConf srcDBConf;
    private ValueFilterManager valueFilterManager;
    private FieldMapManager fieldMapManager;
//    private SyncData syncData;
    private Reader reader;
    private Writer writer;
    private final Logger logger = Logger.getLogger(Job.class);

    @Override
    public void run() {
        // Job信息打印
        console();

        reader = BaseReader.getInstance(jobConf.jobType, srcDBConf.dbType);
        writer = BaseWriter.getInstance(jobConf.jobType, dstDBConf.dbType);

        reader.setJobConf(jobConf);
        reader.setReaderConf(srcDBConf);
        writer.setWriterConfig(dstDBConf);

        reader.connect();
        writer.connect();

        // 字段信息读取&传入
        reader.readField();
        writer.readField();
        fieldMapManager.setReadFields(reader.getFields());
        fieldMapManager.setWriteFields(writer.getFields());

        // 数据过滤在读取时完成
        reader.setValueFilterManager(valueFilterManager);

        // 监听器注册，数据发生变化时执行后续流程
        reader.registerListener(event -> {
            SyncData syncData = event.getSyncData();
            fieldMapManager.run(syncData);
            writer.write(syncData);
        });

        switch (jobConf.jobType) {
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
        int interval = jobConf.getInterval();

        while (true) {
            reader.read();
            try {
                TimeUnit.MINUTES.sleep(interval);
            }
            catch (InterruptedException e) {
                logger.error(e);
            }
        }
    }

    public void runReal() {
        reader.read();
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

    public JobConf getJobConf() {
        return jobConf;
    }

    public void setJobConf(JobConf jobConf) {
        this.jobConf = jobConf;
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
