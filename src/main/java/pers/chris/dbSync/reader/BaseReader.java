package pers.chris.dbSync.reader;

import pers.chris.dbSync.common.ProcedureEvent;
import pers.chris.dbSync.common.ProcedureListener;
import pers.chris.dbSync.common.typeEnum.DBTypeEnum;
import pers.chris.dbSync.common.Readable;
import pers.chris.dbSync.common.Executor;
import pers.chris.dbSync.common.typeEnum.JobTypeEnum;
import pers.chris.dbSync.util.ClassLoadUtil;


public abstract class BaseReader extends Executor implements Readable {

    public static Reader getInstance(JobTypeEnum jobType, DBTypeEnum dbType) {
        switch (jobType) {
            case REAL:
                return ClassLoadUtil.loadReaderClass(dbType);
            case TIMED:
            default:
                return new Reader();
        }
    }

}