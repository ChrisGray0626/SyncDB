import com.chris.reader.SQLServerReader;
import com.chris.syncData.SyncData;
import com.chris.writer.PostgreSQLWriter;
import org.apache.log4j.Logger;

public class DBSync03 {

    private static final Logger logger = Logger.getLogger(DBSync03.class);

    public static void main(String[] args) {
        String configFileName = "resources/conf03.properties";
        SyncData syncData = new SyncData();
        SQLServerReader sqlServerReader = new SQLServerReader();
        PostgreSQLWriter postgreSQLWriter = new PostgreSQLWriter();

        syncData.config(configFileName);

        postgreSQLWriter.config(configFileName);
        postgreSQLWriter.setSyncData(syncData);
        postgreSQLWriter.connect();
        postgreSQLWriter.write();

        sqlServerReader.config(configFileName);
        sqlServerReader.setSyncData(syncData);
        sqlServerReader.connect();
        sqlServerReader.read();

    }
}
