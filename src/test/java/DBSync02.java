import com.chris.reader.PostgreSQLReader;
import com.chris.syncData.SyncData;
import com.chris.writer.PostgreSQLWriter;
import org.apache.log4j.Logger;

public class DBSync02 {

    private static final Logger logger = Logger.getLogger(DBSync02.class);

    public static void main(String[] args) {
        String configFileName = "resources/conf02.properties";
        SyncData syncData = new SyncData();
        PostgreSQLReader postgreSQLReader = new PostgreSQLReader();
        PostgreSQLWriter postgreSQLWriter = new PostgreSQLWriter();

        postgreSQLWriter.config(configFileName);
        postgreSQLWriter.init(syncData);
        postgreSQLWriter.connect();
        postgreSQLWriter.write();

        postgreSQLReader.config(configFileName);
        postgreSQLReader.initSyncData(syncData);
        postgreSQLReader.connect();
        while (true) {
            postgreSQLReader.read();
        }
    }
}
