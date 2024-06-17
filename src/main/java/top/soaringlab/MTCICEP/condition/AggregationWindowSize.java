package top.soaringlab.MTCICEP.condition;

public enum AggregationWindowSize {
    ms,
    seconds,
    minutes,
    hours,
    days,
    month,
    years;

    public Long getTimeScale() {
        switch (this) {
            case ms: return 1L;
            case seconds: return 1000L;
            case minutes: return 60 * 1000L;
            case hours: return 60 * 60 * 1000L;
            case days: return 24 * 60 * 60 * 1000L;
            case month: return 30 * 24 * 60 * 60 * 1000L;
            case years: return 12 * 30 * 24 * 60 * 60 * 1000L;
            default: throw new IllegalArgumentException();
        }
    }
    @Override
    public String toString() {
        switch (this) {
            case ms: return "ms";
            case seconds: return "seconds";
            case minutes: return "minutes";
            case hours: return "hours";
            case days: return "days";
            case month: return "month";
            case years: return "years";
            default: throw new IllegalArgumentException();
        }
    }
    public Long getWindowSize() {
        switch (this) {
            case ms: return 100 * getTimeScale();
            case seconds:
            case minutes: return 6 * getTimeScale();
            case hours:
            case days:
                return 3 * getTimeScale();
            case month:
            case years:
                return 2 * getTimeScale();
            default: throw new IllegalArgumentException();
        }
    }

    public Long getRightEdge(Long s) {
        return (s / this.getWindowSize() + 1) * this.getWindowSize();
    }

    public Long getAggregationBFSize() {
        switch (this) {
            case ms: return seconds.getTimeScale();
            case seconds: return minutes.getTimeScale();
            case minutes: return hours.getTimeScale();
            case hours: return days.getTimeScale();
            case days: return month.getTimeScale();
            case month: return years.getTimeScale();
            case years: return 10 * years.getWindowSize();
            default: throw new IllegalArgumentException();
        }
    }
}
