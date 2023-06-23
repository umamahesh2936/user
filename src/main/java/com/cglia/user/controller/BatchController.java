package com.cglia.user.controller;

import org.springframework.batch.core.BatchStatus;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cglia.user.config.UserCsvBatchConfig;

@RestController
@RequestMapping("/jobs")
public class BatchController {
	@Autowired
	private JobLauncher jobLauncher;

	private Job importUserJob;

	private Job exportUserJob;

	private Job xmlToDb;
	private Job dbToXml;
	@Autowired
	UserCsvBatchConfig batchConfig;

	@Autowired
	public BatchController(JobLauncher jobLauncher, @Qualifier("csvtodb") Job importUserJob,
			@Qualifier("dbtocsv") Job exportUserJob, @Qualifier("xmltodb") Job xmlToDb,
			@Qualifier("dbtoxml") Job dbToXml, UserCsvBatchConfig batchConfig) {
		super();
		this.jobLauncher = jobLauncher;
		this.importUserJob = importUserJob;
		this.exportUserJob = exportUserJob;
		this.xmlToDb = xmlToDb;
		this.dbToXml = dbToXml;
		this.batchConfig = batchConfig;
	}

	@PostMapping("/importUsers")
	public void importCsvToDBJob() {
		JobParameters jobParameters = new JobParametersBuilder().addLong("startAt", System.currentTimeMillis())
				.toJobParameters();
		try {
			jobLauncher.run(importUserJob, jobParameters);
		} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
				| JobParametersInvalidException e) {
			e.printStackTrace();
		}
	}

	@GetMapping("/exportUsers")
	public void exportDBJobToCsv() {
		JobParameters jobParameters = new JobParametersBuilder().addLong("startAt", System.currentTimeMillis())
				.toJobParameters();

		try {
			JobExecution jobExecution = jobLauncher.run(batchConfig.exportUserJob(), jobParameters);
			BatchStatus batchStatus = jobExecution.getStatus();

			while (batchStatus.isRunning()) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				batchStatus = jobExecution.getStatus();
			}

			if (batchStatus == BatchStatus.COMPLETED) {
				// Job completed successfully, perform any additional logic
				// For example, you can return a success message or download the CSV file
			} else {
				// Job failed, handle the failure scenario
				// For example, you can return an error message or log the failure
			}
		} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
				| JobParametersInvalidException e) {
			e.printStackTrace();
		}
	}

	@PostMapping("/usersxmltodb")
	public ResponseEntity<String> startXmlJob() {
		try {
			// Generate unique job parameters
			JobParameters jobParameters = new JobParametersBuilder().addLong("time", System.currentTimeMillis())
					.toJobParameters();

			// Run the XML import job
			JobExecution jobExecution = jobLauncher.run(xmlToDb, jobParameters);

			return ResponseEntity.status(HttpStatus.OK)
					.body("XML Batch Job started. JobExecutionId: " + jobExecution.getId());
		} catch (Exception e) {
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body("Error starting XML Batch Job: " + e.getMessage());
		}
	}

	@GetMapping("/userdbtoxml")
	public ResponseEntity<String> convertDbToXml() {
		try {
			JobParameters jobParameters = new JobParametersBuilder().addLong("time", System.currentTimeMillis())
					.toJobParameters();
			JobExecution jobExecution = jobLauncher.run(dbToXml, jobParameters);
			return ResponseEntity.status(HttpStatus.OK)
					.body("Db to XML conversion started. JobExecutionId: " + jobExecution.getId());
		} catch (Exception e) {
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body("Error converting Db to XML: " + e.getMessage());
		}
	}
}
