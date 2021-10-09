package pers.chris.dbSync.conf;


import pers.chris.dbSync.common.typeEnum.JobTypeEnum;

public class JobConf {

    public JobTypeEnum jobType;
    public String dstDBConfId;
    public String srcDBConfId;
    public String syncConfId;

}
