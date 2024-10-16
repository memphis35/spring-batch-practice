package com.example.batch_jobs.sensors.configuration;

import com.example.batch_jobs.sensors.domain.AbnormalTemperatureAlert;
import com.example.batch_jobs.sensors.domain.DailySensorAggregatedTemperatures;
import com.example.batch_jobs.sensors.domain.DailySensorTemperatures;
import com.thoughtworks.xstream.security.ExplicitTypePermission;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.xml.builder.StaxEventItemReaderBuilder;
import org.springframework.batch.item.xml.builder.StaxEventItemWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.oxm.Marshaller;
import org.springframework.oxm.Unmarshaller;
import org.springframework.oxm.xstream.XStreamMarshaller;
import org.springframework.transaction.PlatformTransactionManager;

import java.math.BigDecimal;
import java.math.MathContext;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Configuration
@Profile("sensorJob")
public class TemperatureSensorJobConfiguration {

    @Bean
    public Job processTemperatureSensor(@Qualifier("sensorFileToXml") Step readDataAndFormXml,
                                        @Qualifier("xmlFileToReport") Step analyseXmlAndReport,
                                        JobRepository jobRepository) {
        return new JobBuilder("aggregate_and_report_xml", jobRepository)
                .start(readDataAndFormXml)
                .next(analyseXmlAndReport)
                .build();
    }

    @Bean
    public Step sensorFileToXml(@Qualifier("sensorFileReader") ItemReader<DailySensorTemperatures> reader,
                                @Qualifier("sensorDataProcessor") ItemProcessor<DailySensorTemperatures, DailySensorAggregatedTemperatures> processor,
                                @Qualifier("dailyTemperatureXmlWriter") ItemWriter<DailySensorAggregatedTemperatures> writer,
                                JobRepository jobRepository,
                                PlatformTransactionManager transactionManager) {
        return new StepBuilder("aggregate_sensor_data", jobRepository)
                .<DailySensorTemperatures, DailySensorAggregatedTemperatures>chunk(1, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .allowStartIfComplete(false)
                .build();
    }

    @Bean
    public ItemReader<DailySensorTemperatures> sensorFileReader(@Value("classpath:temperatures") Resource origin) {
        final DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer(":");
        delimitedLineTokenizer.setNames("date", "measures");
        return new FlatFileItemReaderBuilder<DailySensorTemperatures>()
                .name("sensor_data_reader")
                .resource(origin)
                .lineTokenizer(delimitedLineTokenizer)
                .fieldSetMapper(fieldSet -> {
                    final String date = fieldSet.readString("date");
                    final String temperatures = fieldSet.readString("measures");
                    final List<BigDecimal> temperatureList = Arrays.stream(temperatures.split(","))
                            .map(BigDecimal::new)
                            .toList();
                    return new DailySensorTemperatures(LocalDate.from(DateTimeFormatter.ISO_DATE.parse(date)), temperatureList);
                })
                .build();
    }

    @Bean
    public ItemProcessor<DailySensorTemperatures, DailySensorAggregatedTemperatures> sensorDataProcessor() {
        return item -> {
            final String date = DateTimeFormatter.ISO_ORDINAL_DATE.format(item.date());
            final BigDecimal min = item.temperatures().stream().min(Comparator.naturalOrder()).orElse(BigDecimal.ZERO);
            final BigDecimal sum = item.temperatures().stream().reduce(BigDecimal.ZERO, BigDecimal::add);
            final BigDecimal count = BigDecimal.valueOf(item.temperatures().size());
            final BigDecimal max = item.temperatures().stream().max(Comparator.naturalOrder()).orElse(BigDecimal.ZERO);
            return new DailySensorAggregatedTemperatures(date, min, sum.divide(count, new MathContext(2)), max);
        };
    }

    @Bean
    public ItemWriter<?> dailyTemperatureXmlWriter(@Qualifier("dailyTemperatureXmlMarshaller") Marshaller marshaller,
                                                   @Value("file:report.xml") WritableResource destination) {
            return new StaxEventItemWriterBuilder<DailySensorTemperatures>()
                    .name("aggregated_temperatures_xml_writer")
                    .resource(destination)
                    .rootTagName("temperatures")
                    .marshaller(marshaller)
                    .overwriteOutput(true)
                    .build();
    }

    @Bean
    public XStreamMarshaller dailyTemperatureXmlMarshaller() {
        final XStreamMarshaller marshaller = new XStreamMarshaller();
        final Map<String, Class<?>> aliases = Map.of(
                "measure", DailySensorAggregatedTemperatures.class,
                "date", String.class,
                "min", BigDecimal.class,
                "avg", BigDecimal.class,
                "max", BigDecimal.class);
        final ExplicitTypePermission explicitTypePermission = new ExplicitTypePermission(
                new Class[]{DailySensorAggregatedTemperatures.class}
        );
        marshaller.setAliases(aliases);
        marshaller.setTypePermissions(explicitTypePermission);
        return marshaller;
    }

    @Bean
    public Step xmlFileToReport(@Qualifier("xmlAggregatedResultReader") ItemReader<DailySensorAggregatedTemperatures> reader,
                                @Qualifier("abnormalTemperatureAnalyser") ItemProcessor<DailySensorAggregatedTemperatures, AbnormalTemperatureAlert> processor,
                                @Qualifier("abnormalTemperatureCsvWriter") ItemWriter<AbnormalTemperatureAlert> writer,
                                JobRepository jobRepository,
                                PlatformTransactionManager transactionManager) {
        return new StepBuilder("analyse_xml_and_report", jobRepository)
                .<DailySensorAggregatedTemperatures, AbnormalTemperatureAlert>chunk(1, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public ItemReader<DailySensorAggregatedTemperatures> xmlAggregatedResultReader(@Qualifier("dailyTemperatureXmlMarshaller") Unmarshaller marshaller,
                                                                                   @Value("file:report.xml") WritableResource origin) {
        return new StaxEventItemReaderBuilder<DailySensorAggregatedTemperatures>()
                .name("aggregated_temperatures_xml_reader")
                .resource(origin)
                .unmarshaller(marshaller)
                .addFragmentRootElements("measure")
                .build();
    }

    @Bean
    public ItemProcessor<DailySensorAggregatedTemperatures, AbnormalTemperatureAlert> abnormalTemperatureAnalyser() {
        return item -> {
            final BigDecimal averageBaseLine = BigDecimal.valueOf(-50L, 0);
            final var average = item.getAvg().add(averageBaseLine);
            final boolean avgExceeded = average.doubleValue() > 0.0;
            return avgExceeded ? new AbnormalTemperatureAlert(item.getDate(), average.toEngineeringString()) : null;
        };
    }

    @Bean
    public ItemWriter<AbnormalTemperatureAlert> abnormalTemperatureCsvWriter(@Value("file:alert.csv") WritableResource resource) {
        return new FlatFileItemWriterBuilder<AbnormalTemperatureAlert>()
                .name("abnormal_temperatures_csv_writer")
                .resource(resource)
                .delimited()
                .delimiter(",")
                .names("date", "averageExceeded")
                .build();
    }
}
