package com.example.batch_jobs.coins.threaded.multi;

import com.example.batch_jobs.coins.GeneralCoinsJobConfiguration;
import com.example.batch_jobs.coins.domain.Coin;
import com.example.batch_jobs.coins.domain.PlayerScore;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@Profile("multi-thread")
public class MultiThreadCoinsJobConfiguration extends GeneralCoinsJobConfiguration {

    @Bean
    public Job singleThreadCoinScoreCalculation(@Qualifier("calculateTotalScores") Step step,
                                                JobRepository jobRepository) {
        return new JobBuilder("calculate_player_scores_in_a_few_threads", jobRepository)
                .start(step)
                .build();
    }

    @Bean
    public Step calculateTotalScores(@Qualifier("coinTablePagingItemReader") ItemReader<Coin> reader,
                                     @Qualifier("coinScoreAggregator") ItemProcessor<Coin, PlayerScore> processor,
                                     @Qualifier("playerScoreItemWriter") ItemWriter<PlayerScore> writer,
                                     @Qualifier("taskExecutor") TaskExecutor taskExecutor,
                                     JobRepository jobRepository,
                                     PlatformTransactionManager transactionManager) {
        return new StepBuilder("calculate_player_scores", jobRepository)
                .<Coin, PlayerScore>chunk(1, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .taskExecutor(taskExecutor)
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        final ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(4);
        taskExecutor.setMaxPoolSize(4);
        return taskExecutor;
    }
}
