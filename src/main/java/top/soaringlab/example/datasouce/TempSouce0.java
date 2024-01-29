package top.soaringlab.example.datasouce;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import top.soaringlab.example.oj.TemptureSensor;

import java.util.Calendar;
import java.util.Random;

public class TempSouce0 implements SourceFunction<TemptureSensor> {
    // 申明一个标志位
    private Boolean running = true;

    @Override
    public void run(SourceContext<TemptureSensor> sourceContext) throws Exception {
        // 随机生成数据
        Random random = new Random();
        // 定义id选取的数据集
        Integer[] sensorId = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        // 定义环境温度
        double ambient = -185;
        // 定义BoxId
        Integer[] boxId = {0,1,2};

        while (running) {
            // 随机生成正常数据
            // 为每个储气罐生成对应传感器数据
            for (int k : boxId){
                for (int i : sensorId) {
                    double tempture = ambient * (random.nextDouble() * 0.05 + 0.95);
                    Long timestamp = Calendar.getInstance().getTimeInMillis();
                    sourceContext.collect(new TemptureSensor(i, tempture, timestamp, k));
                }
            }


            Thread.sleep(100L);
        }


    }

    @Override
    public void cancel() {
        running = false;
    }
}
