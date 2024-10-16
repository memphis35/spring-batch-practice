package com.example.batch_jobs.teams.domain;

import java.io.Serializable;
import java.math.BigDecimal;

public record ScoreRecord(String name,
                          BigDecimal score) implements Comparable<ScoreRecord>, Serializable {
    @Override
    public int compareTo(ScoreRecord o) {
        return this.score.compareTo(o.score);
    }
}
