package top.soaringlab.example.mapper;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import top.soaringlab.example.event.TemperatureEvent;

/**
 * @author rillusory
 * @Description
 * @date 1/26/24 4:56 PM
 **/
public class TemperatureEventRecorder extends RichMapFunction<TemperatureEvent, TemperatureEvent> {
    private transient Meter meter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.meter = getRuntimeContext()
                .getMetricGroup()
                .meter("throughput", new MeterView(5));
        //  .meter("throughput", new  DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    }

    @Override
    public TemperatureEvent map(TemperatureEvent value) throws Exception {
        this.meter.markEvent();
        return value;
    }
}
