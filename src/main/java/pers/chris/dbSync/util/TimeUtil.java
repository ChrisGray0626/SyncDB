package pers.chris.dbSync.util;

import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

public class TimeUtil {

    private TimeUtil() {}
    private static final Logger logger = Logger.getLogger(TimeUtil.class);

    // 当前时间 - 时间间隔
    public static String intervalTime(Integer interval) {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.MINUTE, -interval);
        String pattern = "yyyy-MM-dd HH:mm:ss";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

        return "'" + simpleDateFormat.format(calendar.getTime()) + "'";
    }

    public static void sleep(Integer minute) {
        try {
            TimeUnit.MINUTES.sleep(minute);
        }
        catch (InterruptedException e) {
            logger.error(e);
        }
    }

}
