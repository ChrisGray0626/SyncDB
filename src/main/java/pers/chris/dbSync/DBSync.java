package pers.chris.dbSync;

import pers.chris.dbSync.job.DBSyncJobManager;
import org.apache.log4j.Logger;


public class DBSync {

    private static final Logger logger = Logger.getLogger(DBSync.class);

    public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        String confFileName = "resources/conf.properties";
        DBSyncJobManager dbSyncJobManager = new DBSyncJobManager();

        dbSyncJobManager.config(confFileName);
        dbSyncJobManager.configJob();
        dbSyncJobManager.run();
    }
}
