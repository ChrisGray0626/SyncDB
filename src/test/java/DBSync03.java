import com.chris.reader.SQLSeverReader;
import com.chris.syncData.SyncData;
import com.chris.writer.PostgreSQLWriter;
import org.apache.log4j.Logger;

public class DBSync03 {

    private static final Logger logger = Logger.getLogger(DBSync03.class);

    public static void main(String[] args) {
        String configFileName = "resources/conf03.properties";
        SyncData syncData = new SyncData();
        SQLSeverReader sqlSeverReader = new SQLSeverReader();
        PostgreSQLWriter postgreSQLWriter = new PostgreSQLWriter();

        syncData.config(configFileName);

        postgreSQLWriter.config(configFileName);
        postgreSQLWriter.init(syncData);
        postgreSQLWriter.connect();
        postgreSQLWriter.write();

        sqlSeverReader.config(configFileName);
        sqlSeverReader.initSyncData(syncData);
        sqlSeverReader.connect();
        sqlSeverReader.read();

    }
}
