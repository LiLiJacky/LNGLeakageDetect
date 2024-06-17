package top.soaringlab.test.weather.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @author rillusory
 * @Description
 * @date 4/23/24 3:53 PM
 **/
public class Common {
    public static double parseDoubleOrDefault(String str) {
        double defaultValue = 0.0;
        if (str == null || str.isEmpty()) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(str);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public static Long convertStringToLong(String str) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime dateTime = LocalDateTime.parse(str, formatter);

        // Assuming the system default time zone
        long timestamp = dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

        return timestamp;
    }

    public static Long dayToMillseconds(int days) {
        return  hoursToMillseconds(24 * days);
    }

    public static Long hoursToMillseconds(int hours) {
        return 1L * hours * 60 * 60 * 1000;
    }
}
