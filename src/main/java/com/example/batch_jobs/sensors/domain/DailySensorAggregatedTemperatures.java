package com.example.batch_jobs.sensors.domain;

import java.math.BigDecimal;

public class DailySensorAggregatedTemperatures {
    private String date;
    private BigDecimal min;
    private BigDecimal avg;
    private BigDecimal max;

    public DailySensorAggregatedTemperatures(String date, BigDecimal min, BigDecimal avg, BigDecimal max) {
        this.date = date;
        this.min = min;
        this.avg = avg;
        this.max = max;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public BigDecimal getMin() {
        return min;
    }

    public void setMin(BigDecimal min) {
        this.min = min;
    }

    public BigDecimal getAvg() {
        return avg;
    }

    public void setAvg(BigDecimal avg) {
        this.avg = avg;
    }

    public BigDecimal getMax() {
        return max;
    }

    public void setMax(BigDecimal max) {
        this.max = max;
    }
}
