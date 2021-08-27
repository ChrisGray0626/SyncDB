import org.apache.log4j.Logger;
import org.junit.Test;
import org.postgresql.PGNotification;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ListenNotifyTest {

    public static Logger logger = Logger.getLogger(ListenNotifyTest.class);

    public static void main(String[] args)
    {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            logger.debug(e);
        }
        String url = "jdbc:postgresql://pgm-bp12544lafqu69199o.pg.rds.aliyuncs.com:1921/postgres";

        Connection lConn = null;
        Connection nConn = null;
        try {
            lConn = DriverManager.getConnection(url,"postgre","zjplan@2021");
            nConn = DriverManager.getConnection(url,"postgre","zjplan@2021");
            Listener listener = new Listener(lConn);
            Notifier notifier = new Notifier(nConn);
            listener.start();
            notifier.start();
        } catch (SQLException e) {
            logger.debug(e);
        }
    }
}

class Listener extends Thread
{
    private Logger logger = Logger.getLogger(Listener.class);
    private Connection conn;
    private org.postgresql.PGConnection pgconn;

    Listener(Connection conn) throws SQLException
    {
        this.conn = conn;
        this.pgconn = conn.unwrap(org.postgresql.PGConnection.class);
        Statement stmt = conn.createStatement();
        stmt.execute("LISTEN mymessage");
        stmt.close();
    }

    public void run()
    {
        try
        {
            while (true)
            {
                // If this thread is the only one that uses the connection, a timeout can be used to
                // receive notifications immediately:
                PGNotification[] notifications = pgconn.getNotifications();
                if (notifications != null)
                {
                    for (PGNotification notification : notifications)
                        System.out.println("Got notification: " + notification.getName());
                }
                Thread.sleep(500);
            }
        }
        catch (SQLException | InterruptedException e)
        {
            logger.debug(e);
        }
    }
}

class Notifier extends Thread
{
    private Logger logger = Logger.getLogger(Notifier.class);
    private Connection conn;

    public Notifier(Connection conn)
    {
        this.conn = conn;
    }

    public void run()
    {
        while (true)
        {
            try
            {
                Statement stmt = conn.createStatement();
                stmt.execute("NOTIFY mymessage, 'Hello World'");
                stmt.close();
                Thread.sleep(2000);
            }
            catch (SQLException | InterruptedException e)
            {
                logger.debug(e);
            }
        }
    }

}
