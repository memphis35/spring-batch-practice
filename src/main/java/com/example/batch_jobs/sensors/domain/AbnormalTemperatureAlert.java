package com.example.batch_jobs.sensors.domain;

public record AbnormalTemperatureAlert(
        String date,
        String averageExceeded
) {
}
