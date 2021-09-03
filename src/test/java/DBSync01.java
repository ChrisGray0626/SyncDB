import com.chris.reader.MySQLReader;
import com.chris.syncData.SyncData;
import com.chris.writer.PostgreSQLWriter;
import org.apache.log4j.Logger;


public class DBSync01 {

    private static final Logger logger = Logger.getLogger(DBSync01.class);

    public static void main(String[] args) {
        String configFileName = "resources/conf01.properties";
        SyncData syncData = new SyncData();
        PostgreSQLWriter postgreSQLWriter = new PostgreSQLWriter();
        MySQLReader mySQLReader = new MySQLReader();

        postgreSQLWriter.config(configFileName);
        postgreSQLWriter.setSyncData(syncData);
        postgreSQLWriter.connect();
        postgreSQLWriter.write();

        mySQLReader.config(configFileName);
        mySQLReader.setSyncData(syncData);
        mySQLReader.connect();
        mySQLReader.read();
    }
}
