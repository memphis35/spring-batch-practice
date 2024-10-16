package com.example.batch_jobs.teams.domain;

import java.util.List;

public record TeamScores(
        String team,
        List<TeamPlayer> players
) {
}
