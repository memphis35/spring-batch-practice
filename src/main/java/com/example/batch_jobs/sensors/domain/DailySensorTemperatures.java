package com.example.batch_jobs.sensors.domain;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

public record DailySensorTemperatures(
        LocalDate date,
        List<BigDecimal> temperatures
) {
}
