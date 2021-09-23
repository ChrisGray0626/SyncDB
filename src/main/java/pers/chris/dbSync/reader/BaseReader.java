package pers.chris.dbSync.reader;

import pers.chris.dbSync.common.DBTypeEnum;
import pers.chris.dbSync.common.Readable;
import pers.chris.dbSync.common.Executor;
import pers.chris.dbSync.job.JobTypeEnum;
import pers.chris.dbSync.util.ClassLoadUtil;


public abstract class BaseReader extends Executor implements Readable {

    public static Reader getInstance(JobTypeEnum jobType, DBTypeEnum dbType) {
        switch (jobType) {
            case PUSH:
                return ClassLoadUtil.loadReaderClass(dbType);
            case PULL:
            default:
                return new Reader();
        }
    }

}