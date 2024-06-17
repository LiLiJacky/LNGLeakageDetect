package top.soaringlab.MTCICEP.generator;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import top.soaringlab.MTCICEP.bloomfilter.BloomFilter;
import top.soaringlab.MTCICEP.event.EventRecord;
import top.soaringlab.MTCICEP.event.IntervalEvent;
import top.soaringlab.MTCICEP.event.RawEvent;
import top.soaringlab.MTCICEP.processor.MTCIntervalToBloomFilterProcessFunction;

import java.io.Serializable;
import java.util.BitSet;

/**
 * @author rillusory
 * @Description
 * @date 1/31/24 2:04 PM
 **/
public class EventRecordGenerator<I extends IntervalEvent, E extends EventRecord> implements Serializable {
    private Class<I> sourceTypeClass;
    private Class<E> targetTypeClass;
    private DataStream<I> sourceStream;
    private DataStream<E> targetStream;
    private double fPositiveProbability;
    private int expectedNumberOfFilterElements;
    public EventRecordGenerator sourceType(Class<I> sourceTyp) {
        this.sourceTypeClass = sourceTyp;
        return this;
    }

    public EventRecordGenerator source(DataStream<I> srcStream) {
        this.sourceStream = srcStream;
        return this;
    }
    public EventRecordGenerator targetType(Class<E> targetType) {
        this.targetTypeClass = targetType;
        return this;
    }
    public EventRecordGenerator fPositive(Double fp) {
        this.fPositiveProbability = fp;
        return this;
    }
    public EventRecordGenerator expectedNum(Integer i) {
        this.expectedNumberOfFilterElements = i;
        return this;
    }

    public DataStream<E> run() throws Exception {
        BloomFilter<String> bf = new BloomFilter<>(fPositiveProbability, expectedNumberOfFilterElements);

        if (sourceStream instanceof KeyedStream) {
            targetStream = ((KeyedStream) sourceStream)
                    .process(new MTCIntervalToBloomFilterProcessFunction(bf, sourceTypeClass, targetTypeClass), TypeInformation.of(targetTypeClass));
        } else {
            targetStream = sourceStream.keyBy(v -> v.getKey())
                    .process(new MTCIntervalToBloomFilterProcessFunction(bf, sourceTypeClass, targetTypeClass), TypeInformation.of(targetTypeClass));

        }

        return targetStream;
    }



}
