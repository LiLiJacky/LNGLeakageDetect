package top.soaringlab.example.cep;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import top.soaringlab.example.oj.AbnormalLog;
import top.soaringlab.example.oj.TemptureSensor;

import java.util.Properties;

public class RegonizeLayer {
    public static void main(String[] args) throws Exception{
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
                "tempturesensor",
                new SimpleStringSchema(),
                properties
        ));

        // stream.print("213");

        SingleOutputStreamOperator<TemptureSensor> disDerializeTemStream = stream.map(new MapFunction<String, TemptureSensor>() {
            @Override
            public TemptureSensor map(String s) throws Exception {
                s.replace("TemptureSensor{", "");
                String[] ans = s.split(",");
                Integer sensorid = Integer.parseInt(ans[0].substring(ans[0].indexOf("=") + 1, ans[0].length()));
                Double tempture = Double.parseDouble(ans[1].substring(ans[1].indexOf("=") + 1, ans[1].length()));
                Long timestamp = Long.parseLong(ans[2].substring(ans[2].indexOf("=") + 1, ans[2].length()));
                Integer boxId = Integer.parseInt(ans[3].substring(ans[3].indexOf("=") + 1, ans[3].length() - 1));

                TemptureSensor now = new TemptureSensor(sensorid, tempture, timestamp, boxId);

                return now;
            }
        });

        // disDerializeTemStream.print("反序列化：");

        SingleOutputStreamOperator<AbnormalLog> abLog = disDerializeTemStream.keyBy(data -> data.boxId)
                .flatMap(new RichFlatMapFunction<TemptureSensor, AbnormalLog>() {
                    MapState<Integer, Tuple2<Long, Double>> allLayerTemp;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<Integer, Tuple2<Long, Double>> layertemp = new MapStateDescriptor<Integer, Tuple2<Long, Double>>("layertemp", Types.INT , Types.TUPLE(Types.LONG, Types.DOUBLE));
                        allLayerTemp = getRuntimeContext().getMapState(layertemp);
                    }

                    @Override
                    public void flatMap(TemptureSensor temptureSensor, Collector<AbnormalLog> collector) throws Exception {
                        Integer nowId = temptureSensor.sensorid;
                        Tuple2<Long, Double> topLayer = allLayerTemp.get(nowId + 1);
                        Tuple2<Long, Double> downLayer = allLayerTemp.get(nowId - 1);

                        if (topLayer != null) {
                            if (temptureSensor.timestamp <= topLayer.f0) {
                                double chazhi = 0;
                                if (temptureSensor.tempture < topLayer.f1) chazhi = topLayer.f1 - temptureSensor.tempture;
                                else chazhi = temptureSensor.tempture - topLayer.f1;
                                if (chazhi > 14) {
                                    AbnormalLog newLog = new AbnormalLog(nowId+1, nowId, topLayer.f1, temptureSensor.tempture, temptureSensor.timestamp, chazhi, temptureSensor.boxId);
                                    collector.collect(newLog);
                                }

                            }
                        }

                        if (downLayer != null) {
                            if (temptureSensor.timestamp <= downLayer.f0) {
                                double chazhi = 0;
                                if (temptureSensor.tempture < downLayer.f1) chazhi = downLayer.f1 - temptureSensor.tempture;
                                else chazhi = temptureSensor.tempture - downLayer.f1;
                                if (chazhi > 14) {
                                    AbnormalLog newLog = new AbnormalLog(nowId, nowId-1, temptureSensor.tempture, downLayer.f1, temptureSensor.timestamp, chazhi, temptureSensor.boxId);
                                    collector.collect(newLog);
                                }
                            }
                        }

                        Tuple2<Long, Double> nowLayer = new Tuple2<>(temptureSensor.timestamp, temptureSensor.tempture);
                        allLayerTemp.put(nowId, nowLayer);

                    }
                });

        abLog.print("abnormalLog:");
        abLog.map(new MapFunction<AbnormalLog, String>() {
                    @Override
                    public String map(AbnormalLog abnormalLog) throws Exception {
                        return abnormalLog.toString();
                    }
                })
                .addSink(new FlinkKafkaProducer<String>(
                "abnormallog",
                new SimpleStringSchema(),
                properties
        ));


        env.execute();
    }
}
