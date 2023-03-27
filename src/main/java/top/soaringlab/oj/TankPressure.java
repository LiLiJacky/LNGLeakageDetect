package top.soaringlab.oj;

public class TankPressure {
    public Integer boxId;
    public Double pressure;
    public Long timeStamp;

    public TankPressure() {
    }

    public TankPressure(Integer boxId, Double pressure, Long timeStamp) {
        this.boxId = boxId;
        this.pressure = pressure;
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "TankPressure{" +
                "boxId=" + boxId +
                ", pressure=" + pressure +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
