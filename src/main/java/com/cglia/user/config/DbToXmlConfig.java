package com.cglia.user.config;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.oxm.xstream.XStreamMarshaller;

import com.cglia.user.dto.User;

@Configuration
@EnableBatchProcessing
/** this class is for fetching the data from the database to xml */
public class DbToXmlConfig {
	private final JobBuilderFactory jobBuilderFactory;
	private final StepBuilderFactory stepBuilderFactory;
	private final DataSource dataSource;

	public DbToXmlConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory,
			DataSource dataSource) {
		this.jobBuilderFactory = jobBuilderFactory;
		this.stepBuilderFactory = stepBuilderFactory;
		this.dataSource = dataSource;
	}

	@Bean
	public StaxEventItemWriter<User> dbToXmlWriter() {
		StaxEventItemWriter<User> writer = new StaxEventItemWriter<>();
		writer.setResource(new FileSystemResource("useroutputxml.xml"));

		Map<String, String> aliasesMap = new HashMap<>();
		aliasesMap.put("user", "com.cglia.user.dto.User");
		XStreamMarshaller marshaller = new XStreamMarshaller();
		marshaller.setAliases(aliasesMap);
		writer.setMarshaller(marshaller);
		writer.setRootTagName("users");
		writer.setOverwriteOutput(true);
		return writer;
	}

	@Bean
	public JdbcCursorItemReader<User> dbToXmlReader() {
		JdbcCursorItemReader<User> reader = new JdbcCursorItemReader<>();
		reader.setDataSource(dataSource);
		reader.setSql("select * from user");
		reader.setRowMapper(new RowMapper<User>() {

			@Override
			public User mapRow(ResultSet rs, int rowNum) throws SQLException {
				User user = new User();
				user.setId(rs.getInt(1));
				user.setName(rs.getString(2));
				user.setPhone(rs.getLong(3));
				user.setStatus(rs.getInt(4));
				return user;
			}
		});
		return reader;
	}

	@Bean
	public ItemProcessor<User, User> userProcessorForXmlToDb() {
		return user -> {
			// Apply processing logic to the user object if needed
			// For example, you can modify or filter the user object here
			return user;
		};
	}

	@Bean
	public Step stepForDbToXml() {
		return stepBuilderFactory.get("stepForDbToXml").<User, User>chunk(10).reader(dbToXmlReader())
				.processor(userProcessorForXmlToDb()).writer(dbToXmlWriter()).build();
	}

	@Bean
	@Qualifier("dbtoxml")
	public Job exportUserToXml() {
		return jobBuilderFactory.get("exportUserToXml").incrementer(new RunIdIncrementer()).flow(stepForDbToXml()).end()
				.build();
	}
}
