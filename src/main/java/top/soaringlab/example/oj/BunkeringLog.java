package top.soaringlab.example.oj;

public class BunkeringLog {
    public Integer oldLayer;
    public Integer newLayer;
    public Long timestamp;
    public Long bunkeringLogId;
    public Integer boxId = 1;
    public String type;

    public BunkeringLog() {
    }

    public BunkeringLog(Integer oldLayer, Integer newLayer, Long timestamp, Long bunkeringLogId, String type) {
        this.oldLayer = oldLayer;
        this.newLayer = newLayer;
        this.timestamp = timestamp;
        this.bunkeringLogId = bunkeringLogId;
        this.type = type;
    }

    @Override
    public String toString() {
        return "BunkeringLog{" +
                "oldLayer=" + oldLayer +
                ", newLayer=" + newLayer +
                ", timestamp=" + timestamp +
                ", bunkeringLogId=" + bunkeringLogId +
                ", boxId=" + boxId +
                ", type='" + type + '\'' +
                '}';
    }
}
