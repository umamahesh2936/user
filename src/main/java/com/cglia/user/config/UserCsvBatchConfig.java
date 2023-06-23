package com.cglia.user.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import com.cglia.user.dto.User;

@Configuration
@EnableBatchProcessing
/** this class is for fetching the data from the database to csv */
public class UserCsvBatchConfig {
//db to csv
	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private DataSource dataSource;

	@Bean
	public JdbcCursorItemReader<User> dbReader() {
		JdbcCursorItemReader<User> cursorItemReader = new JdbcCursorItemReader<>();
		cursorItemReader.setDataSource(dataSource);
		cursorItemReader.setSql("SELECT id, name, phone, status FROM user");
		cursorItemReader.setRowMapper(new UserRowMapper());
		return cursorItemReader;
	}

	@Bean
	public FlatFileItemWriter<User> csvWriter() {
		FlatFileItemWriter<User> writer = new FlatFileItemWriter<>();
		writer.setResource(new FileSystemResource("src/main/resources/users.csv"));

		DelimitedLineAggregator<User> lineAggregator = new DelimitedLineAggregator<>();
		lineAggregator.setDelimiter(",");

		BeanWrapperFieldExtractor<User> fieldExtractor = new BeanWrapperFieldExtractor<>();
		fieldExtractor.setNames(new String[] { "id", "name", "phone", "status" });
		lineAggregator.setFieldExtractor(fieldExtractor);

		writer.setLineAggregator(lineAggregator);
		return writer;
	}

	@Bean
	public Step step2() {
		return stepBuilderFactory.get("step2").<User, User>chunk(10).reader(dbReader()).writer(csvWriter()).build();
	}

	@Bean
	@Qualifier("dbtocsv")
	public Job exportUserJob() {
		return jobBuilderFactory.get("exportUserJob").incrementer(new RunIdIncrementer()).flow(step2()).end().build();
	}
}
