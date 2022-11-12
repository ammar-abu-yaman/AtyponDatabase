package com.atypon.project.worker;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.query.Query;
import com.atypon.project.worker.query.QueryType;
import com.atypon.project.worker.handler.QueryHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.List;

@SpringBootApplication
public class WorkerApplication {

	public static void main(String[] args) throws Exception {
		DatabaseManager.initialize();
		loadData();
		SpringApplication.run(WorkerApplication.class, args);


	}

	static void loadData() throws JsonProcessingException {
		DatabaseManager manager = DatabaseManager.getInstance();

		Query request = Query.builder()
				.originator(Query.Originator.Broadcaster)
				.queryType(QueryType.CreateIndex)
				.databaseName("Books")
				.indexFieldName("price")
				.build();

		QueryHandler chain = manager.getHandlersFactory().getHandler(request);
		chain.handle(request);
	}

}
