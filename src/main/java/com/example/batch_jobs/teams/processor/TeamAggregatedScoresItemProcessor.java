package com.example.batch_jobs.teams.processor;

import com.example.batch_jobs.teams.domain.TeamScores;
import com.example.batch_jobs.teams.domain.ScoreRecord;
import com.example.batch_jobs.teams.domain.TeamAverageScore;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Component
@StepScope
public class TeamAggregatedScoresItemProcessor implements ItemProcessor<TeamScores, TeamAverageScore> {

    private final Integer scoreRank;
    private ExecutionContext executionContext;

    public TeamAggregatedScoresItemProcessor(@Value("#{jobParameters['scoreRank']}") Integer scoreRank) {
        this.scoreRank = scoreRank;
    }

    @Override
    public TeamAverageScore process(TeamScores item) {
        final ScoreRecord maxScoreRecord = executionContext.get("maxScoreRecord", ScoreRecord.class, new ScoreRecord("John Doe", BigDecimal.ZERO));
        final ScoreRecord minScoreRecord = executionContext.get("minScoreRecord", ScoreRecord.class, new ScoreRecord("John Doe", BigDecimal.valueOf(Long.MAX_VALUE)));
        final BigDecimal playersCount = BigDecimal.valueOf(item.players().size());
        final BigDecimal averageScore = item.players().stream()
                .map(teamPlayer -> {
                    final BigDecimal score = teamPlayer.scores().get(scoreRank);
                    final ScoreRecord scoreRecord = new ScoreRecord(teamPlayer.name(), score);
                    if (scoreRecord.compareTo(maxScoreRecord) > 0) {
                        executionContext.put("maxScoreRecord", scoreRecord);
                    }
                    if (scoreRecord.compareTo(minScoreRecord) < 0) {
                        executionContext.put("minScoreRecord", scoreRecord);
                    }
                    return score;
                })
                .reduce(BigDecimal.ZERO, BigDecimal::add)
                .divide(playersCount, 2, RoundingMode.HALF_UP);

        return new TeamAverageScore(item.team(), averageScore);
    }

    public void setExecutionContext(ExecutionContext executionContext) {
        this.executionContext = executionContext;
    }
}
