package com.example.batch_jobs.coins.partitioned.local;

import com.example.batch_jobs.coins.GeneralCoinsJobConfiguration;
import com.example.batch_jobs.coins.domain.Coin;
import com.example.batch_jobs.coins.domain.PlayerScore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.lang.NonNull;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Configuration
@Profile("partitioned")
public class LocalPartitionedCoinsJobConfiguration extends GeneralCoinsJobConfiguration {

    @Bean
    public Job job(@Qualifier("partitionStep") Step partitionerStep,
                   JobRepository jobRepository) {
        return new JobBuilder("calculate_coins_in_partitions", jobRepository)
                .start(partitionerStep)
                .build();
    }

    @Bean
    public Step partitionStep(@Qualifier("simpleStep") Step simpleStep,
                              @Qualifier("simplePartitioner") Partitioner partitioner,
                              JobRepository jobRepository,
                              @Qualifier("taskExecutor") TaskExecutor taskExecutor) {
        return new StepBuilder("partition_step", jobRepository)
                .partitioner("simple_step", partitioner)
                .step(simpleStep)
                .taskExecutor(taskExecutor)
                .gridSize(3)
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public Partitioner simplePartitioner() {
        return gridSize -> IntStream.range(0, gridSize)
                .mapToObj(index -> {
                    final ExecutionContext partitionExecutionContext = new ExecutionContext();
                    partitionExecutionContext.putInt("partitionCount", gridSize);
                    partitionExecutionContext.putInt("partitionIndex", index % gridSize);
                    partitionExecutionContext.putString("partitionKey", "PartitionExecutionContext-%s".formatted(index));
                    return partitionExecutionContext;
                })
                .collect(Collectors.toMap(context -> context.getString("partitionKey"), Function.identity()));
    }

    @Bean
    public Step simpleStep(@Qualifier("partitionReader") ItemReader<Coin> reader,
                           @Qualifier("coinScoreAggregator") ItemProcessor<Coin, PlayerScore> processor,
                           @Qualifier("playerScoreItemWriter") ItemWriter<PlayerScore> writer,
                           JobRepository jobRepository,
                           PlatformTransactionManager transactionManager) {
        return new StepBuilder("simple_step", jobRepository)
                .<Coin, PlayerScore>chunk(10, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .listener(new StepExecutionListener() {
                    private final Logger log = LoggerFactory.getLogger(this.getClass());
                    @Override
                    public void beforeStep(@NonNull StepExecution stepExecution) {
                        final ExecutionContext executionContext = stepExecution.getExecutionContext();
                        final Object partitionCount = executionContext.get("partitionCount");
                        final Object partitionIndex = executionContext.get("partitionIndex");
                        log.info("Partitioned step started its execution at thread {} with index {} of {}", Thread.currentThread().getName(), partitionIndex, partitionCount);
                    }
                })
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    @StepScope
    public ItemReader<Coin> partitionReader(@Value("#{stepExecutionContext['partitionCount']}") Object partitionCount,
                                            @Value("#{stepExecutionContext['partitionIndex']}") Object partitionIndex,
                                            DataSource dataSource) {
        return new JdbcPagingItemReaderBuilder<Coin>()
                .name("coin_table_paging_item_reader")
                .dataSource(dataSource)
                .fetchSize(20)
                .selectClause("id, player_name, coin_type, coin_score")
                .fromClause("coins.collected_coin")
                .whereClause("where abs(('x'||substr(md5(player_name),1,8))::bit(32)::int % " + "%s) = %s".formatted(partitionCount, partitionIndex))
                .sortKeys(Map.of("id", Order.ASCENDING))
                .rowMapper(COIN_ROW_MAPPER)
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor();
        asyncTaskExecutor.setConcurrencyLimit(3);
        return asyncTaskExecutor;
    }
}
