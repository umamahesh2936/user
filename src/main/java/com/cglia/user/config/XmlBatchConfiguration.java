package com.cglia.user.config;

import java.sql.PreparedStatement;
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
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.oxm.xstream.XStreamMarshaller;

import com.cglia.user.dto.User;

@Configuration
@EnableBatchProcessing
/** this class is for saving the xml data into database */
public class XmlBatchConfiguration {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private DataSource dataSource;

	/*********************************************/
	// XML To DB

	@Bean
	public StaxEventItemReader<User> xmlReader() {
		StaxEventItemReader<User> reader = new StaxEventItemReader<>();
		reader.setResource(new ClassPathResource("userinputxml.xml"));
		reader.setFragmentRootElementName("user");

		Map<String, String> aliasesMap = new HashMap<>();
		aliasesMap.put("user", "com.cglia.user.dto.User");
		XStreamMarshaller marshaller = new XStreamMarshaller();
		marshaller.setAliases(aliasesMap);
		marshaller.getXStream().allowTypes(new Class[] { User.class });

		reader.setUnmarshaller(marshaller);
		return reader;
	}

	@Bean
	public JdbcBatchItemWriter<User> xmlWriter() {
		JdbcBatchItemWriter<User> writer = new JdbcBatchItemWriter<>();
		writer.setDataSource(dataSource);
		writer.setSql("INSERT INTO user(id, name, phone, status) " + "VALUES (?, ?, ?,?)");
		writer.setItemPreparedStatementSetter(new UserItemPreparedStatementSetter());
		return writer;
	}

	@Bean
	public Step stepForXmlToDb() {
		return stepBuilderFactory.get("stepForXmlToDb").<User, User>chunk(10).reader(xmlReader())
				.processor(userProcessorForXmlToDb()).writer(xmlWriter()).build();
	}

	@Bean
	@Qualifier("xmltodb")
	public Job importUserFromXml() {
		return jobBuilderFactory.get("importUserFromXml").incrementer(new RunIdIncrementer()).flow(stepForXmlToDb())
				.end().build();
	}

	@Bean
	public ItemProcessor<User, User> userProcessorForXmlToDb() {
		return user -> {
			return user;
		};
	}

	private static class UserItemPreparedStatementSetter implements ItemPreparedStatementSetter<User> {
		@Override
		public void setValues(User user, PreparedStatement preparedStatement) throws SQLException {
			preparedStatement.setInt(1, user.getId());
			preparedStatement.setString(2, user.getName());
			preparedStatement.setLong(3, user.getPhone());
			preparedStatement.setInt(4, user.getStatus());

		}
	}

}