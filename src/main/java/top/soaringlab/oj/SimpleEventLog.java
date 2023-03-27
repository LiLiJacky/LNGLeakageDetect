package top.soaringlab.oj;

public class SimpleEventLog {
    public String eventType;
    public Long beginTime;
    public Long endTime;
    public Integer boxId;
    public String details;

    public SimpleEventLog() {
    }

    public SimpleEventLog(String eventType, Long beginTime, Long endTime, Integer boxId, String details) {
        this.eventType = eventType;
        this.beginTime = beginTime;
        this.endTime = endTime;
        this.boxId = boxId;
        this.details = details;
    }

    @Override
    public String toString() {
        return "SimpleEventLog{" +
                "eventType='" + eventType + '\'' +
                ", beginTime=" + beginTime +
                ", endTime=" + endTime +
                ", boxId=" + boxId +
                ", details='" + details + '\'' +
                '}';
    }
}
