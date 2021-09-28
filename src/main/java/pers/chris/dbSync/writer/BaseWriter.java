package pers.chris.dbSync.writer;

import pers.chris.dbSync.common.typeEnum.DBTypeEnum;
import pers.chris.dbSync.common.Executor;
import pers.chris.dbSync.common.Writeable;
import pers.chris.dbSync.common.typeEnum.JobTypeEnum;
import pers.chris.dbSync.util.ClassLoadUtil;


public abstract class BaseWriter extends Executor implements Writeable {

    public static Writer getInstance(JobTypeEnum jobType, DBTypeEnum dbType) {
        return new Writer();
    }

}
