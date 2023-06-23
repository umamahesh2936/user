package com.cglia.user.config;

import org.springframework.batch.core.Job;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import com.cglia.user.dto.User;
import com.cglia.user.repository.UserRepository;

@Configuration
@EnableBatchProcessing
/** this class is for saving the data from the csv to database */
public class SpringBatchConfig {
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	@Autowired
	private UserRepository userRepository;

	@Bean
	public FlatFileItemReader<User> csvReader() {
		FlatFileItemReader<User> itemReader = new FlatFileItemReader<>();
		itemReader.setResource(new ClassPathResource("userdata.csv"));
		itemReader.setName("csvReader");
		itemReader.setLinesToSkip(1);
		itemReader.setLineMapper(lineMapper());
		return itemReader;
	}

	private LineMapper<User> lineMapper() {
		DefaultLineMapper<User> lineMapper = new DefaultLineMapper<>();

		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
		lineTokenizer.setDelimiter(",");
		lineTokenizer.setStrict(false);
		lineTokenizer.setNames("id", "name", "phone", "status");

		BeanWrapperFieldSetMapper<User> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
		fieldSetMapper.setTargetType(User.class);

		lineMapper.setLineTokenizer(lineTokenizer);
		lineMapper.setFieldSetMapper(fieldSetMapper);
		return lineMapper;
	}

	@Bean
	public ItemProcessor<User, User> processor() {
		// Optional: Add any data transformation or validation logic
		return user -> user;
	}

	@Bean
	public RepositoryItemWriter<User> dbWriter() {
		RepositoryItemWriter<User> writer = new RepositoryItemWriter<>();
		writer.setRepository(userRepository);
		writer.setMethodName("save");
		return writer;
	}

	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1").<User, User>chunk(10).reader(csvReader()).processor(processor())
				.writer(dbWriter()).build();
	}

	@Bean
	@Qualifier("csvtodb")
	public Job importUserJob() {
		return jobBuilderFactory.get("importUserJob").flow(step1()).end().build();
	}
}
