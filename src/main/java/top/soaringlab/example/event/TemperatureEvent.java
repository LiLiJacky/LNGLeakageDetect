package top.soaringlab.example.event;

import top.soaringlab.MTCICEP.event.RawEvent;

/**
 * @author rillusory
 * @Description
 * @date 1/26/24 12:46 PM
 **/
public class TemperatureEvent extends RawEvent {
    public TemperatureEvent() {

    }
    public TemperatureEvent(String id , long ts, double v) {
        super(id, ts, v);
    }
    public TemperatureEvent(String id , long ts, double v, String tc) {
        super(id, ts, v, tc);
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
        return "TemperatureEvent("  + getValue() + " , " + getTimestamp() + ")";
    }
}
