package top.soaringlab.sink;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import top.soaringlab.datasouce.Bunkering;
import top.soaringlab.datasouce.ContainPressure;
import top.soaringlab.datasouce.RollOver;
import top.soaringlab.datasouce.TempSouce0;
import top.soaringlab.oj.BunkeringLog;
import top.soaringlab.oj.TankPressure;
import top.soaringlab.oj.TemptureSensor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class SinkToKafka {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "10.176.34.214:9092");


        // 罐柜内传感器温度写入
        SingleOutputStreamOperator<TemptureSensor> tempSource = env.addSource(new TempSouce0())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<TemptureSensor>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<TemptureSensor>() {
                            @Override
                            public long extractTimestamp(TemptureSensor temptureSensor, long l) {
                                return temptureSensor.timestamp;
                            }
                        }));
        SingleOutputStreamOperator<TemptureSensor> rollOver = env.addSource(new RollOver())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<TemptureSensor>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<TemptureSensor>() {
                    @Override
                    public long extractTimestamp(TemptureSensor temptureSensor, long l) {
                        return temptureSensor.timestamp;
                    }
                }));

        // rollOver.print("异常");

        SingleOutputStreamOperator<TemptureSensor> stream = tempSource.keyBy(data -> data.boxId).keyBy(data -> data.sensorid).connect(rollOver.keyBy(data -> data.boxId).keyBy(data -> data.sensorid))
                .process(new CoProcessFunction<TemptureSensor, TemptureSensor, TemptureSensor>() {
                    // 设置一个状态List记录当前来自于异常数据源的数据
                    // 对与其中的状态设置一个10s的过期时限
                    // 当超过10s没有接收到来自异常源的数据时，状态过期
                    ListState<Integer> rollOver;
                    ValueState<Long> lastModified;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ListStateDescriptor<Integer> rollover = new ListStateDescriptor<>("rollover", Integer.class);
                        rollOver = getRuntimeContext().getListState(rollover);
                        ValueStateDescriptor<Long> lastmodified = new ValueStateDescriptor<>("lastmodified", Long.class);
                        lastModified = getRuntimeContext().getState(lastmodified);
                    }

                    @Override
                    public void processElement1(TemptureSensor temptureSensor, CoProcessFunction<TemptureSensor, TemptureSensor, TemptureSensor>.Context context, Collector<TemptureSensor> collector) throws Exception {
                        Integer nowId = temptureSensor.sensorid;
                        boolean hasId = false;
                        for (Integer id : rollOver.get()) {
                            if (id.equals(nowId)) {
                                hasId = true;
                            }
                        }

                        if (!hasId) {
                            collector.collect(temptureSensor);
                        }
                    }

                    @Override
                    public void processElement2(TemptureSensor temptureSensor, CoProcessFunction<TemptureSensor, TemptureSensor, TemptureSensor>.Context context, Collector<TemptureSensor> collector) throws Exception {
                        Integer nowId = temptureSensor.sensorid;
                        boolean hasId = false;
                        for (Integer id : rollOver.get()) {
                            if (id.equals(nowId)) {
                                hasId = true;
                            }
                        }
                        if (!hasId) {
                            rollOver.add(nowId);
                            lastModified.update(temptureSensor.timestamp);
                            context.timerService().registerEventTimeTimer(lastModified.value() + 150L);
                        }

                        collector.collect(temptureSensor);
                    }

                    @Override
                    public void onTimer(long timestamp, CoProcessFunction<TemptureSensor, TemptureSensor, TemptureSensor>.OnTimerContext ctx, Collector<TemptureSensor> out) throws Exception {
                        lastModified.clear();
                        rollOver.clear();
                    }
                });


        SingleOutputStreamOperator<String> result = stream
                .map(new MapFunction<TemptureSensor, String>() {
                    @Override
                    public String map(TemptureSensor temptureSensor) throws Exception {
                        return temptureSensor.toString();
                    }
                });
        result.print("Sensor data: ");
        result
                .addSink(new FlinkKafkaProducer<String>(
                        "tempturesensor",
                        new SimpleStringSchema(),
                        properties
                ));

        // 加注日志写入
        SingleOutputStreamOperator<BunkeringLog> bunkeringLogDataStreamSource = env.addSource(new Bunkering())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<BunkeringLog>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<BunkeringLog>() {
                            @Override
                            public long extractTimestamp(BunkeringLog bunkeringLog, long l) {
                                return bunkeringLog.timestamp;
                            }
                        }));

        SingleOutputStreamOperator<String> bunkering = bunkeringLogDataStreamSource.keyBy(data -> data.boxId).map(new BunkeringMapFunction<BunkeringLog, String>());
        bunkering
                .addSink(new FlinkKafkaProducer<String>(
                        "bunkeringLog",
                        new SimpleStringSchema(),
                        properties
                ));

        // 压力日志写入
        env.addSource(new ContainPressure())
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<TankPressure>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<TankPressure>() {
                                    @Override
                                    public long extractTimestamp(TankPressure tankPressure, long l) {
                                        return tankPressure.timeStamp;
                                    }
                                })).map(new MapFunction<TankPressure, String>() {
                    @Override
                    public String map(TankPressure tankPressure) throws Exception {
                        return tankPressure.toString();
                    }
                }).addSink(new FlinkKafkaProducer<String>(
                        "tankpressurelog",
                        new SimpleStringSchema(),
                        properties
                ));

        env.execute();

    }

    public static class BunkeringMapFunction<B, S> extends RichMapFunction<BunkeringLog, String> {
        ListState<Integer> layer;

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<Integer> layerD = new ListStateDescriptor<Integer>("layer", Integer.class);
            layer = getRuntimeContext().getListState(layerD);
        }

        @Override
        public String map(BunkeringLog bunkeringLog) throws Exception {
            Integer oid = bunkeringLog.oldLayer;

            // 判断是否是加注行为
            if (oid.equals(-1)) {
                Iterable<Integer> nowlayer = layer.get();
                // System.out.println(nowlayer);
                if (!nowlayer.iterator().hasNext()) oid = 0;
                else for (Integer i : nowlayer) oid = i;
                bunkeringLog.oldLayer = oid;
                return  bunkeringLog.toString();
            }

            // 处理消耗行为
            //System.out.println(bunkeringLog.toString() + "consume");
            ArrayList<Integer> newLayer = Lists.newArrayList();
            Integer nid = bunkeringLog.newLayer;
            for (Integer i : layer.get()) {
                    if (nid <= i) break;
                    newLayer.add(i);
            }
            newLayer.add(nid);
            //System.out.println(newLayer);
            layer.update(newLayer);
            return  bunkeringLog.toString();
        }
    }
    
}
