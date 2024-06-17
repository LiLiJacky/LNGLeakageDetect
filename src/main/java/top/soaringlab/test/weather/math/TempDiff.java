package top.soaringlab.test.weather.math;

import org.apache.flink.table.functions.ScalarFunction;
import top.soaringlab.test.weather.event.TemperatureEvent;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @author rillusory
 * @Description
 * @date 6/4/24 3:38 PM
 **/
public class TempDiff extends ScalarFunction {
    public TempDiff() {
        super();
    }

    public boolean eval(List<Double> temps, List<Long> ts_list) {
        List<Timestamp> tsts = new ArrayList<>();
        for (long i : ts_list) {
            tsts.add(new Timestamp(i));
        }
        for (int i = 0; i < temps.size(); i++) {
            for (int j = i + 1; j < temps.size(); j++) {
                long diffInDays = Duration.between(tsts.get(i).toInstant(), tsts.get(j).toInstant()).toDays();
                double tempDiff = temps.get(j) - temps.get(i);
                if (diffInDays <= 5 && tempDiff < -20) {
                    return true;
                }
            }
        }
        return false;
    }
}
