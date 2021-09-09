package pers.chris.dbSync.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class TimeUtil {

    // 当前时间 - 时间间隔
    public static String intervalTime(Integer interval) {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.MINUTE, -interval);
        String pattern = "yyyy-MM-dd hh:mm:ss";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

        return simpleDateFormat.format(calendar);
    }
}
