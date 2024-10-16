package com.example.batch_jobs.teams.domain;

import java.math.BigDecimal;

public abstract class TeamAggregatedScore {

    protected final String team;
    protected final Type type;
    protected final BigDecimal score;

    protected TeamAggregatedScore(String team, Type type, BigDecimal score) {
        this.team = team;
        this.type = type;
        this.score = score;
    }

    protected enum Type {
        MIN, AVG, MAX
    }

    public String getTeam() {
        return team;
    }

    public BigDecimal getScore() {
        return score;
    }
}
