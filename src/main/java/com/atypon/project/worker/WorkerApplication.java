package com.atypon.project.worker;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.request.DatabaseRequest;
import com.atypon.project.worker.request.RequestHandler;
import com.atypon.project.worker.request.RequestType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class WorkerApplication {

	public static void main(String[] args) throws Exception {
		DatabaseManager.initialize();
		loadData();
		SpringApplication.run(WorkerApplication.class, args);
	}

	static void loadData() throws JsonProcessingException {
		DatabaseManager manager = DatabaseManager.getInstance();

		DatabaseRequest request = DatabaseRequest.builder()
				.originator(DatabaseRequest.Originator.Broadcaster)
				.requestType(RequestType.CreateIndex)
				.databaseName("Books")
				.indexFieldName("price")
				.build();

		RequestHandler chain = manager.getHandlersFactory().getHandler(request);
		chain.handleRequest(request);

//		for(String content: new String[] {
//				"{\"title\": \"Harry Potter\", \"price\": 13.5}",
//				"{\"title\": \"Jamaica\", \"price\": 13.5}",
//				"{\"title\": \"C++\", \"price\": 200}",
//				"{\"title\": \"Java\", \"price\": 200}",
//		}) {
//			chain.handleRequest(DatabaseRequest
//					.builder()
//					.databaseName("Books")
//					.requestType(RequestType.AddDocument)
//					.payload(new ObjectMapper().readTree(content))
//					.build()
//			);
//		}
	}

}
