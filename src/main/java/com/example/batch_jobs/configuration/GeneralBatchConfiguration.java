package com.example.batch_jobs.configuration;

import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.boot.autoconfigure.batch.BatchDataSource;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import javax.sql.DataSource;

@Configuration
public class GeneralBatchConfiguration {

    @BatchDataSource
    public DataSource springBatchDataSource(DataSourceProperties dataSourceProperties) {
        final SingleConnectionDataSource dataSource = new SingleConnectionDataSource();
        dataSource.setDriverClassName(dataSourceProperties.getDriverClassName());
        dataSource.setUrl(dataSourceProperties.getUrl());
        dataSource.setUsername(dataSource.getUsername());
        dataSource.setPassword(dataSource.getPassword());
        dataSource.setSuppressClose(true);
        return dataSource;
    }

    @Bean
    public JobLauncher asynchronousJobLauncher(JobRepository jobRepository) {
        final TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
        final TaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(taskExecutor);
        return jobLauncher;
    }
}
