package pers.chris.dbSync.conf;


import pers.chris.dbSync.common.typeEnum.JobTypeEnum;

public class JobConf {

    public JobTypeEnum jobType;
    private String dstDBConfId;
    private String srcDBConfId;
    private Integer interval;
    private String timeField; // 记录时间的字段名

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

    public Integer getInterval() {
        return interval;
    }

    public void setInterval(Integer interval) {
        this.interval = interval;
    }

    public String getTimeField() {
        return timeField;
    }

    public void setTimeField(String timeField) {
        this.timeField = timeField;
    }

}
