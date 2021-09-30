package pers.chris.dbSync.conf;


import pers.chris.dbSync.common.typeEnum.JobTypeEnum;

public class JobConf {

    public JobTypeEnum jobType;
    private String dstDBConfId;
    private String srcDBConfId;
    private String syncConfId;

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

    public String getSyncConfId() {
        return syncConfId;
    }

    public void setSyncConfId(String syncConfId) {
        this.syncConfId = syncConfId;
    }

}
