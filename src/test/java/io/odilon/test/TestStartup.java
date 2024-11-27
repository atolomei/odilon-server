package io.odilon.test;


import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import io.odilon.log.Logger;
import io.odilon.service.ObjectStorageService;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;


//@SpringBootTest
public class TestStartup {

		static private Logger logger =	Logger.getLogger(TestStartup.class.getName());

		@Autowired
		ObjectStorageService ds;

		@Autowired
		private VirtualFileSystemService virtualFileSystemService;
		
		@Autowired
		private TestRestTemplate template;
		
		public TestStartup(ObjectStorageService ds, VirtualFileSystemService virtualFileSystemService) {
			this.ds=ds;
			this.virtualFileSystemService=virtualFileSystemService;
		}
		
		@Test
		public void runTest() {
			
			/**
			List<String> result = new ArrayList<String>();

			logger.debug(ds.toString());
			result.add(ds.toString());
			
			return result;
			**/
		   //ResponseEntity<String> response = template.getForEntity("/bucket/list", String.class);
	       // assertThat(response.getBody()).isEqualTo("Greetings from Spring Boot!");
	       //logger.debug(response.toString());
			
			logger.debug( ds.findAllBuckets() );
			
		}
		
		
}
