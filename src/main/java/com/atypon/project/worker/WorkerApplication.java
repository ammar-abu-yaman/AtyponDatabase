package com.atypon.project.worker;

import com.atypon.project.worker.core.DatabaseManager;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;


@SpringBootApplication
public class WorkerApplication {


	public static void main(String[] args) {
		// initialize database internals
		DatabaseManager.initialize();
		// start spring application
		ConfigurableApplicationContext x = SpringApplication.run(WorkerApplication.class, args);
	}

}
