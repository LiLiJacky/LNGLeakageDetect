package top.soaringlab.example.datasouce;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import top.soaringlab.example.oj.BunkeringLog;

import java.util.Calendar;
import java.util.Random;

public class Bunkering implements SourceFunction<BunkeringLog> {
    boolean running = true;
    @Override
    public void run(SourceContext<BunkeringLog> sourceContext) throws Exception {
        // 随机选取id进行异常模拟
        Random random = new Random();
        // 定义id的数据集
        int[] sensorId = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

        // 每1000s生成一次加注记录 每1000s生成一次抽取记录
        while (running) {
            // 首先模拟一次加满
            Integer oldLayer = -1;
            Integer newLayer = 9;
            Long timestamp = Calendar.getInstance().getTimeInMillis();
            Long id = random.nextLong();

            sourceContext.collect(new BunkeringLog(oldLayer, newLayer, timestamp, id, "add"));
            Thread.sleep(1000L);

            // 模拟一次抽取
            oldLayer = 9;
            newLayer = random.nextInt(sensorId.length);
            timestamp = Calendar.getInstance().getTimeInMillis();
            id = random.nextLong();
            sourceContext.collect(new BunkeringLog(oldLayer, newLayer, timestamp, id, "reduce"));

            Thread.sleep(300L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
