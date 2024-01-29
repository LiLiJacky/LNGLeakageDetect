package top.soaringlab.example.oj;

public class AbnormalLog {
    public Integer upLayerId;
    public Integer downLayerId;
    public Double upLayerTempture;
    public Double downLayerTempture;
    public Long timeStamp;
    public Double temptureDifference;
    public Integer boxId;

    public AbnormalLog() {
    }

    public AbnormalLog(Integer upLayerId, Integer downLayerId, Double upLayerTempture, Double downLayerTempture, Long timeStamp, Double temptureDifference, Integer boxId) {
        this.upLayerId = upLayerId;
        this.downLayerId = downLayerId;
        this.upLayerTempture = upLayerTempture;
        this.downLayerTempture = downLayerTempture;
        this.timeStamp = timeStamp;
        this.temptureDifference = temptureDifference;
        this.boxId = boxId;
    }

    @Override
    public String toString() {
        return "AbnormalLog{" +
                "upLayerId=" + upLayerId +
                ", downLayerId=" + downLayerId +
                ", upLayerTempture=" + upLayerTempture +
                ", downLayerTempture=" + downLayerTempture +
                ", timeStamp=" + timeStamp +
                ", temptureDifference=" + temptureDifference +
                ", boxId=" + boxId +
                '}';
    }
}
