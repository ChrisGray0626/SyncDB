package pers.chris.dbSync;

import pers.chris.dbSync.job.JobManager;
import org.apache.log4j.Logger;


public class DBSync {

    private static final Logger logger = Logger.getLogger(DBSync.class);

    public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        String confFileName = "resources/conf.properties";
        JobManager jobManager = new JobManager();

        jobManager.config(confFileName);
        jobManager.readJobConf();
        jobManager.run();
    }
}
