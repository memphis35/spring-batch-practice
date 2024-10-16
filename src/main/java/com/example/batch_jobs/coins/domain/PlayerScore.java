package com.example.batch_jobs.coins.domain;

import java.math.BigDecimal;

public record PlayerScore(
        String name,
        BigDecimal total
) {
}
