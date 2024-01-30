package top.soaringlab.example.event;

import top.soaringlab.MTCICEP.event.RawEvent;

/**
 * @author rillusory
 * @Description
 * @date 1/29/24 9:21 PM
 **/
public class PressureEvent extends RawEvent {
    public PressureEvent() {
        super();
    }
    public PressureEvent(String id , long ts, double v, String tc) {
        super(id, ts, v, tc);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PressureEvent) {
            PressureEvent other = (PressureEvent) obj;
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
        return obj instanceof PressureEvent;
    }

    @Override
    public String toString() {
        return "PressureEvent("  + getValue() + " , " + getTimestamp() + ")";
    }
}
