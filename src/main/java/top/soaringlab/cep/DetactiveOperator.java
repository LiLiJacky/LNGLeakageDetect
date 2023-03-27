//package top.soaringlab.cep;
//
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//import java.util.Properties;
//
//public class DetactiveOperator {
//    public static void main(String[] args) throws Exception{
//        // abnormallog 是温度差大于 ？
//        // 现在是 温度差 + 压力乘积 > ? = 翻滚
//        // 所以，假定现在的模式是检测到 翻滚时 发送翻滚的box id
//
////        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
////        env.setParallelism(1);
////
////        Properties properties = new Properties();
////        properties.setProperty("bootstrap.servers", "10.176.34.214:9092");
////        properties.setProperty("group.id", "consumer-group");
////        properties.setProperty("key.deserializer",
////                "org.apache.kafka.common.serialization.StringDeserializer");
////        properties.setProperty("value.deserializer",
////                "org.apache.kafka.common.serialization.StringDeserializer");
////        properties.setProperty("auto.offset.reset", "latest");
//
//
//}
