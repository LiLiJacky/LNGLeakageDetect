package top.soaringlab.example.event;

import top.soaringlab.MTCICEP.event.IntervalEvent;

import java.io.Serializable;

/**
 * @author rillusory
 * @Description
 * @date 1/26/24 3:53 PM
 **/
public class PressureWarning extends IntervalEvent implements Serializable {
    public PressureWarning() {
        super();
    }
    public PressureWarning(long sts, long ets, double value, String valueDescriptor, String key, String tc) {
        super(sts, ets, value, valueDescriptor, key, tc);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PressureWarning) {
            PressureWarning other = (PressureWarning) obj;
            return getValue() == other.getValue() && this.getStartTimestamp() == other.getStartTimestamp()
                    && this.getEndTimestamp() == other.getEndTimestamp() && this.getKey().equals(other.getKey());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Double.hashCode(getValue());
    }

    @Override
    public String toString()
    {
        return super.toString().replace("IntervalEvent","PressureWarning");
    }

}


