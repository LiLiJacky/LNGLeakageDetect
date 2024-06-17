package top.soaringlab.MTCICEP.event;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.StringValue;

import java.util.BitSet;

/**
 * @author rillusory
 * @Description
 * @date 1/31/24 2:15 PM
 **/
public class EventRecord extends Tuple5<StringValue, IntValue, LongValue, StringValue, StringValue> {
    public EventRecord() {
        f1 = new IntValue(Integer.MIN_VALUE);
        f2 = new LongValue(Long.MIN_VALUE);
        f3 = new StringValue("ms");
        f0 = new StringValue("");
        f4 = new StringValue("dummy");
    }
    public EventRecord(String bv, int size, long ts, String tc, String k) {
        this.f0 = new StringValue(bv);
        this.f1 = new IntValue(size);
        this.f2 = new LongValue(ts);
        this.f3 = new StringValue(tc);
        this.f4 = new StringValue(k);
    }

    public long getTimestamp() {return f2.getValue();}
    public String getKey() {return  f4.getValue();}
    public BitSet getBitSet() {
        BitSet bitSet = new BitSet();

        for (int i = 0; i < f0.length(); i++) {
            if (f0.charAt(i) == '1') {
                bitSet.set(i);
            }
        }

        return bitSet;
    }
    public Integer getSize() {return f1.getValue();}
    public String getTimeScale() {return f4.getValue();}
    public boolean equals(Object obj) {
        if (!(obj instanceof EventRecord)) return false;
        EventRecord other = (EventRecord) obj;

        return other.getBitSet().equals(this.getBitSet()) && other.getKey().equals(this.getKey())
                && other.getTimestamp() == this.getTimestamp() && other.getTimeScale().equals(this.getTimeScale())
                && other.getSize() == other.getSize();
    }
}
