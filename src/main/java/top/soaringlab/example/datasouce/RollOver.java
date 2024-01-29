package top.soaringlab.example.datasouce;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import top.soaringlab.example.oj.TemptureSensor;

import java.util.Calendar;
import java.util.Random;

public class RollOver implements SourceFunction<TemptureSensor> {
    // 标志位
    private  boolean running = true;

    @Override
    public void run(SourceContext<TemptureSensor> sourceContext) throws Exception {
        // 随机选取id进行异常模拟
        Random random = new Random();
        // 定义id的数据集
        Integer[] sensorId = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        // 定义环境温度
        double ambient = -170;
        // box的数据集
        Integer[] boxId = {0,1,2};

        // 每10s生成一个异常数据
        while (running) {
            // 异常box
            int bid = boxId[random.nextInt(boxId.length)];
            // 异常层级
            int id = sensorId[random.nextInt(sensorId.length)];
            // 假设每次异常会生成40-50条数据
            int count = 40 + random.nextInt(10);
            while (count > 0) {
                double tempture = ambient * (random.nextDouble() * 0.05 + 0.95);
                Long timestamp = Calendar.getInstance().getTimeInMillis();
                sourceContext.collect(new TemptureSensor(id, tempture, timestamp, bid));
                Thread.sleep(100L);
                count--;
            }
            Thread.sleep(300L);
        }



    }

    @Override
    public void cancel() {
        running = false;
    }
}
