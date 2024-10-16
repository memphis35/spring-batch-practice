package com.example.batch_jobs.teams.domain;

import java.math.BigDecimal;

public record TeamPercentageScore(
        String team,
        BigDecimal percentage
) {
}
