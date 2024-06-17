package top.soaringlab.test.weather.math;

import org.apache.flink.calcite.shaded.com.google.common.collect.Multiset;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.*;
import org.apache.flink.types.Row;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author rillusory
 * @Description
 * @date 5/24/24 3:10 PM
 **/
public class MannKendall extends ScalarFunction {
    private List<Double> data;

    public MannKendall(List<Double> data) {
        this.data = data;
    }

    public MannKendall() {

    }

    public MannKendall(double[] data) {
        for (double i : data) {
            this.data.add(i);
        }
    }

    public double calculateS() {
        int n = data.size();
        int s = 0;

        for (int i = 0; i < n - 1; i++) {
            for (int j = i + 1; j < n; j++) {
                if (data.get(j) > data.get(i)) {
                    s += 1;
                } else if (data.get(j) < data.get(i)) {
                    s -= 1;
                }
            }
        }

        return s;
    }

    public double calculateVarianceS() {
        int n = data.size();
        return n * (n - 1) * (2 * n + 5) / 18.0;
    }

    public double calculateZ() {
        double s = calculateS();
        double varianceS = calculateVarianceS();

        if (s > 0) {
            return (s - 1) / Math.sqrt(varianceS);
        } else if (s < 0) {
            return (s + 1) / Math.sqrt(varianceS);
        } else {
            return 0;
        }
    }

    public double eval(Collection<Row> collectedData) {
        List<Double> temperatures = collectedData.stream()
                .map(row -> (Double) row.getField(1)) // assuming the second field is temperature
                .collect(Collectors.toList());
        this.data = temperatures;
        Double score = calculateZ();
        // 在这里实现 Mann-Kendall 测试逻辑
        return score;
    }

}
