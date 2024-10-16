package com.example.batch_jobs.teams.domain;

import java.math.BigDecimal;
import java.util.List;

public record TeamPlayer(
        String name,
        List<BigDecimal> scores
) {
}
