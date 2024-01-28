package io.odilon.test;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.ResponseEntity;

import io.odilon.log.Logger;


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ODTest {

	static private Logger logger = Logger.getLogger(ODTest.class.getName());
	
	@Autowired
	private TestRestTemplate template;

	 @Test
	 public void getHello() throws Exception {
	 
		 ResponseEntity<String> response = template.getForEntity("/bucket/list", String.class);
	       //assertThat(response.getBody()).isEqualTo("Greetings from Spring Boot!");
	       logger.debug(response.toString());
	       logger.debug("here");
	    }
	

}
