package top.soaringlab.example.oj;

public class TemptureSensor {
    public Integer sensorid;
    public Double tempture;
    public Long timestamp;
    public Integer boxId;


    public TemptureSensor() {
    }

    public TemptureSensor(Integer sensorid, Double tempture, Long timestamp, Integer boxId) {
        this.sensorid = sensorid;
        this.tempture = tempture;
        this.timestamp = timestamp;
        this.boxId = boxId;
    }

    @Override
    public String toString() {
        return "TemptureSensor{" +
                "sensorid=" + sensorid +
                ", tempture=" + tempture +
                ", timestamp=" + timestamp +
                ", boxId=" + boxId +
                '}';
    }

}
