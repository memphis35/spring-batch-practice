package com.example.batch_jobs.teams.configuration;

import com.example.batch_jobs.teams.domain.ScoreRecord;
import com.example.batch_jobs.teams.domain.TeamAverageScore;
import com.example.batch_jobs.teams.domain.TeamPercentageScore;
import com.example.batch_jobs.teams.domain.TeamScores;
import com.example.batch_jobs.teams.processor.TeamAggregatedScoresItemProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.CommandRunner;
import org.springframework.batch.core.step.tasklet.JvmCommandRunner;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.builder.MultiResourceItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.task.ThreadPoolTaskExecutorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.function.Function;
import java.util.function.Supplier;

@Configuration
@Profile("performance")
public class TeamPerformanceJobConfiguration {

    private final Function<ScoreRecord, ItemProcessor<TeamAverageScore, TeamPercentageScore>> GET_PROCESSOR =
            scoreRecord -> item -> {
                final BigDecimal percentage = item.getScore()
                        .divide(scoreRecord.score(), 2, RoundingMode.HALF_UP)
                        .multiply(new BigDecimal(100L));
                return new TeamPercentageScore(item.getTeam(), percentage);
            };

    private final Supplier<FlatFileItemWriterBuilder<TeamPercentageScore>> GET_WRITER_BUILDER =
            () -> new FlatFileItemWriterBuilder<TeamPercentageScore>()
                    .formatted()
                    .format("%s: %.2f%%")
                    .names("team", "percentage")
                    .shouldDeleteIfExists(true);

    @Bean
    public Job teamPerformanceJob(@Qualifier("readTeamFiles") Step readTeamStatisticsFile,
                                  @Qualifier("writeMaxStatistics") Step writeMaxStatistics,
                                  @Qualifier("writeMinStatistics") Step writeMinStatistics,
                                  @Qualifier("commandLineCall") Step commandLineCall,
                                  @Qualifier("twoCoresExecutor") TaskExecutor executor,
                                  JobRepository jobRepository) {
        return new JobBuilder("team_performance_report", jobRepository)
                .start(new FlowBuilder<Flow>("read_team_scores_flow").start(readTeamStatisticsFile).build())
                .next(new FlowBuilder<Flow>("write_statistics_in_parallel_flow")
                        .split(executor)
                        .add(
                                new FlowBuilder<Flow>("write_max_flow").start(writeMaxStatistics).build(),
                                new FlowBuilder<Flow>("write_min_flow").start(writeMinStatistics).build()
                        )
                        .build())
                .next(commandLineCall)
                .build()
                .build();
    }

    @Bean
    public ThreadPoolTaskExecutor twoCoresExecutor() {
        return new ThreadPoolTaskExecutorBuilder()
                .corePoolSize(2)
                .build();
    }

    @Bean
    public Step readTeamFiles(@Qualifier("multiResourceItemReader") MultiResourceItemReader<TeamScores> reader,
                              @Qualifier("teamAggregatedScoresItemProcessor") TeamAggregatedScoresItemProcessor processor,
                              @Qualifier("averageTeamScoreFileWriter") ItemWriter<TeamAverageScore> writer,
                              @Qualifier("contextPromotionListener") StepExecutionListener promotionListener,
                              JobRepository jobRepository,
                              PlatformTransactionManager platformTransactionManager) {
        return new StepBuilder("read_team_files", jobRepository)
                .<TeamScores, TeamAverageScore>chunk(1, platformTransactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(@NonNull StepExecution stepExecution) {
                        processor.setExecutionContext(stepExecution.getExecutionContext());
                    }
                })
                .listener(promotionListener)
                .faultTolerant()
                .skip(IndexOutOfBoundsException.class)
                .skipLimit(100)
                .build();
    }

    @Bean
    public StepExecutionListener contextPromotionListener() {
        final ExecutionContextPromotionListener listener = new ExecutionContextPromotionListener();
        listener.setKeys(new String[]{"maxScoreRecord", "minScoreRecord"});
        return listener;
    }

    @Bean
    public MultiResourceItemReader<TeamScores> multiResourceItemReader(@Qualifier("teamScoreFileReader") ResourceAwareItemReaderItemStream<TeamScores> reader,
                                                                       @Value("classpath:teams/scores_*.txt") Resource[] files) {
        return new MultiResourceItemReaderBuilder<TeamScores>()
                .resources(files)
                .delegate(reader)
                .saveState(false)
                .build();
    }

    @Bean
    public ResourceAwareItemReaderItemStream<String> simpleStringLineReader() {
        return new FlatFileItemReaderBuilder<String>()
                .name("simple_line_to_string_file_reader")
                .lineMapper((line, lineNumber) -> line)
                .saveState(false)
                .build();
    }

    @Bean
    public ItemWriter<TeamAverageScore> averageTeamScoreFileWriter(@Value("file:tp1_average_scores.txt") WritableResource destination) {
        return new FlatFileItemWriterBuilder<TeamAverageScore>()
                .name("average_team_score_writer")
                .resource(destination)
                .delimited()
                .delimiter(":")
                .names("team", "score")
                .append(false)
                .shouldDeleteIfExists(true)
                .build();
    }

    @Bean
    public Step writeMaxStatistics(@Qualifier("readAverageScoresForMax") ItemReader<TeamAverageScore> reader,
                                   @Qualifier("calculatePercentageMax") ItemProcessor<TeamAverageScore, TeamPercentageScore> processor,
                                   @Qualifier("writeMaxStats") ItemWriter<TeamPercentageScore> writer,
                                   JobRepository jobRepository,
                                   PlatformTransactionManager platformTransactionManager) {
        return new StepBuilder("write_max_statistics", jobRepository)
                .<TeamAverageScore, TeamPercentageScore>chunk(1, platformTransactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public ItemReader<TeamAverageScore> readAverageScoresForMax(@Value("file:tp1_average_scores.txt") Resource resource) {
        return new FlatFileItemReaderBuilder<TeamAverageScore>()
                .name("average_score_reader_max")
                .delimited()
                .delimiter(":")
                .names("team", "score")
                .fieldSetMapper(fieldSet -> new TeamAverageScore(
                        fieldSet.readString(0),
                        fieldSet.readBigDecimal(1)
                ))
                .resource(resource)
                .build();
    }

    @Bean
    @StepScope
    public ItemProcessor<TeamAverageScore, TeamPercentageScore> calculatePercentageMax(
            @Value("#{jobExecutionContext['maxScoreRecord']}") ScoreRecord maxScore
    ) {
        return GET_PROCESSOR.apply(maxScore);
    }

    @Bean
    public ItemWriter<TeamPercentageScore> writeMaxStats(@Value("file:tp2_max_statistics.txt") WritableResource destination,
                                                         @Qualifier("maxHeaderCallback") FlatFileHeaderCallback callback) {
        return GET_WRITER_BUILDER.get()
                .name("max_stats_writer")
                .resource(destination)
                .headerCallback(callback)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileHeaderCallback maxHeaderCallback(@Value("#{jobExecutionContext['maxScoreRecord']}") ScoreRecord maxScore) {
        return writer -> {
            final String message = "*  The best result is %.2f and was scored by %s!  *\n"
                    .formatted(maxScore.score(), maxScore.name());
            final String delimiter = "*".repeat(message.length() - 1) + "\n";
            writer.write(delimiter);
            writer.write(message);
            writer.write(delimiter);
        };
    }

    @Bean
    public Step writeMinStatistics(@Qualifier("readAverageScoresForMin") ItemReader<TeamAverageScore> reader,
                                   @Qualifier("calculatePercentageMin") ItemProcessor<TeamAverageScore, TeamPercentageScore> processor,
                                   @Qualifier("writeMinStats") ItemWriter<TeamPercentageScore> writer,
                                   JobRepository jobRepository,
                                   PlatformTransactionManager platformTransactionManager) {
        return new StepBuilder("write_min_statistics", jobRepository)
                .<TeamAverageScore, TeamPercentageScore>chunk(1, platformTransactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public ItemReader<TeamAverageScore> readAverageScoresForMin(@Value("file:tp1_average_scores.txt") Resource resource) {
        return new FlatFileItemReaderBuilder<TeamAverageScore>()
                .name("average_score_reader_min")
                .delimited()
                .delimiter(":")
                .names("team", "score")
                .fieldSetMapper(fieldSet -> new TeamAverageScore(
                        fieldSet.readString(0),
                        fieldSet.readBigDecimal(1)
                ))
                .resource(resource)
                .build();
    }

    @Bean
    @StepScope
    public ItemProcessor<TeamAverageScore, TeamPercentageScore> calculatePercentageMin(
            @Value("#{jobExecutionContext['minScoreRecord']}") ScoreRecord minScore
    ) {
        return GET_PROCESSOR.apply(minScore);
    }

    @Bean
    public ItemWriter<TeamPercentageScore> writeMinStats(@Value("file:tp3_min_statistics.txt") WritableResource destination,
                                                         @Qualifier("minHeaderCallback") FlatFileHeaderCallback callback) {
        return GET_WRITER_BUILDER.get()
                .name("min_stats_writer")
                .resource(destination)
                .headerCallback(callback)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileHeaderCallback minHeaderCallback(@Value("#{jobExecutionContext['minScoreRecord']}") ScoreRecord maxScore) {
        return writer -> {
            final String message = "*  The worst result is %.2f and was scored by %s!  *\n"
                    .formatted(maxScore.score(), maxScore.name());
            final String delimiter = "*".repeat(message.length() - 1) + "\n";
            writer.write(delimiter);
            writer.write(message);
            writer.write(delimiter);
        };
    }

    @Bean
    public Step commandLineCall(@Qualifier("commandLineCallTasklet") Tasklet tasklet,
                                JobRepository jobRepository,
                                PlatformTransactionManager platformTransactionManager) {
        return new StepBuilder("command_line_call", jobRepository)
                .tasklet(tasklet, platformTransactionManager)
                .build();
    }

    @Bean
    public Tasklet commandLineCallTasklet() {
        return (contribution, chunkContext) -> {
            final CommandRunner cli = new JvmCommandRunner();
            String[] cmd = {"/bin/bash", "-c", "ls -l > tp4_output.txt"};
            cli.exec(cmd, new String[0], null);
            return RepeatStatus.FINISHED;
        };
    }

}
