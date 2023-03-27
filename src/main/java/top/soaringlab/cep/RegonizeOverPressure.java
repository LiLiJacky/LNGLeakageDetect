package top.soaringlab.cep;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import scala.Int;
import top.soaringlab.oj.SimpleEventLog;
import top.soaringlab.oj.TankPressure;

import java.util.Properties;

public class RegonizeOverPressure {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.176.34.214:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");


        DataStreamSource<String> stream = env.addSource(new
                FlinkKafkaConsumer<String>(
                "tankpressurelog",
                new SimpleStringSchema(),
                properties
        ));

        // stream.print("tankpressurelog");

        stream.map(new MapFunction<String, TankPressure>() {
            @Override
            public TankPressure map(String s) throws Exception {
                s.replace("TankPressure{", "");
                String[] ans = s.split(",");
                Integer boxid = Integer.parseInt(ans[0].substring(ans[0].indexOf("=") + 1, ans[0].length()));
                Double pressure = Double.parseDouble(ans[1].substring(ans[1].indexOf("=") + 1, ans[1].length()));
                Long timestamp = Long.parseLong(ans[2].substring(ans[2].indexOf("=") + 1, ans[2].length() - 1));

                return new TankPressure(boxid, pressure, timestamp);
            }
        }).keyBy(data -> data.boxId).process(new SumWindow(1L))
                .map(new MapFunction<SimpleEventLog, String>() {
                    @Override
                    public String map(SimpleEventLog simpleEventLog) throws Exception {
                        return simpleEventLog.toString();
                    }
                })
                .addSink(new FlinkKafkaProducer<String>(
                        "simpleeventlog",
                        new SimpleStringSchema(),
                        properties
                ));

        env.execute();

    }

    public static class SumWindow extends KeyedProcessFunction<Integer , TankPressure, SimpleEventLog>{
        private Long windowSize; // 窗口大小
        ListState<TankPressure> overPressure;
        ValueState<Long> beginTime;
        ValueState<Long> endTime;

        public SumWindow(Long windowSize) {
            this.windowSize = windowSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<TankPressure> opds = new ListStateDescriptor<TankPressure>("overpressure", TankPressure.class);
            overPressure = getRuntimeContext().getListState(opds);
            ValueStateDescriptor<Long> bt = new ValueStateDescriptor<>("begintime", Types.LONG);
            beginTime = getRuntimeContext().getState(bt);
            ValueStateDescriptor<Long> et = new ValueStateDescriptor<>("endtime", Types.LONG);
            endTime = getRuntimeContext().getState(et);
        }

        @Override
        public void processElement(TankPressure tankPressure, KeyedProcessFunction<Integer, TankPressure, SimpleEventLog>.Context context, Collector<SimpleEventLog> collector) throws Exception {
            if (tankPressure.pressure > 17.2) {
                System.out.println(tankPressure.toString());
                if (beginTime == null) beginTime.update(tankPressure.timeStamp);
                overPressure.add(tankPressure);
                endTime.update(tankPressure.timeStamp);
                // System.out.println(context.getCurrentKey());
                // System.out.println(endTime.value());
                context.timerService().registerEventTimeTimer(endTime.value() + windowSize);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Integer, TankPressure, SimpleEventLog>.OnTimerContext ctx, Collector<SimpleEventLog> out) throws Exception {
            String type = "overPressure";
            // System.out.println("lalla");
            Integer boxId = ctx.getCurrentKey();
            Long begintime = beginTime.value();
            Long endtime = endTime.value();
            String des = "";
            for (TankPressure i : overPressure.get()) {
                String t =Long.toString(i.timeStamp);
                String p = Double.toString(i.pressure);
                des = des + "{timestamp:" + t + ",pressure:" + p +"},";
            }
            des.substring(0, des.length() - 1);
            beginTime.clear();
            endTime.clear();
            overPressure.clear();
            out.collect(new SimpleEventLog(type, begintime, endtime, boxId, des));
        }
    }
}
