package com.example.batch_jobs.teams.domain;

import java.math.BigDecimal;

public class TeamAverageScore extends TeamAggregatedScore {
    public TeamAverageScore(String team, BigDecimal score) {
        super(team, Type.AVG, score);
    }
}
