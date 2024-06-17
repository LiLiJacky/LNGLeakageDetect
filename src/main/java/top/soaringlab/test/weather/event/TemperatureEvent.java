package top.soaringlab.test.weather.event;

import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.StringValue;
import top.soaringlab.MTCICEP.event.RawEvent;

/**
 * @author rillusory
 * @Description
 * @date 4/23/24 12:01 AM
 **/
public class TemperatureEvent extends RawEvent {
    public TemperatureEvent() {
        super();
    }

    public TemperatureEvent(long timestamp, String city, double temperature) {
        f0 = new StringValue(city);
        f1 = new LongValue(timestamp);
        f2 = new DoubleValue(temperature);
        f3 = new StringValue("hour");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TemperatureEvent) {
            TemperatureEvent other = (TemperatureEvent) obj;
            return other.canEquals(this) && super.equals(other) && getValue() == other.getValue();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 41 * super.hashCode() + Double.hashCode(getValue());
    }

    private boolean canEquals(Object obj){
        return obj instanceof TemperatureEvent;
    }
    @Override
    public String toString() {
        return "TemperatureEvent{" +
                "timestamp=" + getTimestamp() +
                ", city='" + getKey() + '\'' +
                ", temperature=" + getValue() +
                '}';
    }
}

