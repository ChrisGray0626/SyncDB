import com.chris.reader.PostgreSQLReader;
import com.chris.syncData.SyncData;
import org.junit.Test;

public class PGReaderTest {

    @Test
    public static void main(String[] args) {
        SyncData syncData = new SyncData();
        PostgreSQLReader postgreSQLReader = new PostgreSQLReader();
        postgreSQLReader.connect();
        postgreSQLReader.readLogicalSlot(syncData);
    }
}
