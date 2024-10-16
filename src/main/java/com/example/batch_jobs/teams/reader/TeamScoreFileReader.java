package com.example.batch_jobs.teams.reader;

import com.example.batch_jobs.teams.domain.TeamScores;
import com.example.batch_jobs.teams.domain.TeamPlayer;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.Resource;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

@Component
@Profile("performance")
public class TeamScoreFileReader implements ResourceAwareItemReaderItemStream<TeamScores> {

    private final ResourceAwareItemReaderItemStream<String> itemReader;

    public TeamScoreFileReader(@Qualifier("simpleStringLineReader") ResourceAwareItemReaderItemStream<String> itemReader) {
        this.itemReader = itemReader;
    }

    @Override
    public TeamScores read() throws Exception {
        TeamScores teamScores = null;
        String currentLine;
        do {
            currentLine = itemReader.read();
            final boolean isNotFinished = currentLine != null && !currentLine.isEmpty();
            if (isNotFinished) {
                if (!currentLine.contains(":")) {
                    teamScores = new TeamScores(currentLine, new ArrayList<>());
                } else {
                    final String[] player = currentLine.split(":");
                    final String name = player[0];
                    final List<BigDecimal> scores = Stream.of(player[1].split(","))
                            .map(Double::valueOf)
                            .map(score -> BigDecimal.valueOf(score).setScale(2, RoundingMode.HALF_UP))
                            .sorted(Comparator.reverseOrder())
                            .toList();
                    teamScores.players().add(new TeamPlayer(name, scores));
                }
            }
        } while (currentLine != null && !currentLine.isEmpty());
        return teamScores;
    }

    @Override
    public void setResource(@NonNull Resource resource) {
        this.itemReader.setResource(resource);
    }

    @Override
    public void open(@NonNull ExecutionContext executionContext) throws ItemStreamException {
        this.itemReader.open(executionContext);
    }

    @Override
    public void close() throws ItemStreamException {
        this.itemReader.close();
    }
}
