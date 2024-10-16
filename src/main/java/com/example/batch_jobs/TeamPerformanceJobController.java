package com.example.batch_jobs;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/jobs/team-performance")
@Profile("performance")
public class TeamPerformanceJobController {

    private final JobLauncher jobLauncher;
    private final Job job;

    public TeamPerformanceJobController(JobLauncher jobLauncher,
                                        @Qualifier("teamPerformanceJob") Job job) {
        this.jobLauncher = jobLauncher;
        this.job = job;
    }

    @GetMapping
    public ResponseEntity<Object> runTeamPerformanceJob(@RequestParam(name = "scoreRank") Integer scoreRank)
            throws JobExecutionException {
        final JobParameters jobParameters = new JobParametersBuilder()
                .addJobParameter("correlationId", UUID.randomUUID(), UUID.class)
                .addJobParameter("scoreRank", scoreRank, Integer.class)
                .toJobParameters();
        jobLauncher.run(job, jobParameters);
        return ResponseEntity.ok(null);
    }
}
