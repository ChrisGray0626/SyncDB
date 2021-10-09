package pers.chris.dbSync.job;

import pers.chris.dbSync.common.typeEnum.JobTypeEnum;
import pers.chris.dbSync.conf.DBConf;
import pers.chris.dbSync.conf.JobConf;
import pers.chris.dbSync.conf.SyncConf;
import pers.chris.dbSync.fieldMap.FieldMapManager;
import pers.chris.dbSync.reader.BaseReader;
import pers.chris.dbSync.reader.Reader;
import pers.chris.dbSync.syncData.SyncData;
import pers.chris.dbSync.util.TimeUtil;
import pers.chris.dbSync.valueFilter.ValueFilterManager;
import pers.chris.dbSync.writer.BaseWriter;
import pers.chris.dbSync.writer.Writer;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Job extends Thread {

    private String jobId;
    private JobConf jobConf;
    private DBConf dstDBConf;
    private DBConf srcDBConf;
    private SyncConf syncConf;
    private ValueFilterManager valueFilterManager;
    private FieldMapManager fieldMapManager;
    private Reader reader;
    private Writer writer;
    public static Map<String, Thread> curThreads = new HashMap<>();
    private final Logger logger = Logger.getLogger(Job.class);

    @Override
    public void run() {
        // Job信息打印
        console();

        // 当前线程存放
        curThreads.put(jobId, Thread.currentThread());

        JobTypeEnum jobType = jobConf.jobType;
        reader = BaseReader.getInstance(jobType, srcDBConf.dbType);
        writer = BaseWriter.getInstance(jobType, dstDBConf.dbType);

        reader.setSyncConf(syncConf);
        reader.setReaderConf(srcDBConf);
        writer.setWriterConfig(dstDBConf);

        reader.connect();
        writer.connect();

        // 字段信息读取&传入
        reader.readField();
        writer.readField();
        fieldMapManager.setReadFields(reader.getFields());
        fieldMapManager.setWriteFields(writer.getFields());
        fieldMapManager.load();

        // 数据过滤在读取时完成
        valueFilterManager.setSyncConf(syncConf);
        reader.setValueFilterManager(valueFilterManager);

        // 监听器注册，数据发生变化时执行后续流程
        reader.registerListener(event -> {
            SyncData syncData = event.getSyncData();
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
        int interval = syncConf.interval;

        // 线程中断信号检测
        while (!Thread.currentThread().isInterrupted()) {
            reader.read();
            TimeUtil.sleep(interval);
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

    public void setDstDBConf(DBConf dstDBConf) {
        this.dstDBConf = dstDBConf;
    }

    public void setSrcDBConf(DBConf srcDBConf) {
        this.srcDBConf = srcDBConf;
    }

    public void setSyncConf(SyncConf syncConf) {
        this.syncConf = syncConf;
    }

    public JobConf getJobConf() {
        return jobConf;
    }

    public void setJobConf(JobConf jobConf) {
        this.jobConf = jobConf;
    }

    public void setValueFilterManager(ValueFilterManager valueFilterManager) {
        this.valueFilterManager = valueFilterManager;
    }

    public void setFieldMapManager(FieldMapManager fieldMapManager) {
        this.fieldMapManager = fieldMapManager;
    }
}
