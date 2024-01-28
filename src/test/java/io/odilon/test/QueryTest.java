package io.odilon.test;


import java.util.HashMap;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import io.odilon.OdilonApplication;
import io.odilon.log.Logger;
import io.odilon.service.ObjectStorageService;
import io.odilon.vfs.model.VirtualFileSystemService;

@RunWith(SpringRunner.class)
//@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK, classes = OdilonApplication.class)
@SpringBootTest(classes = OdilonApplication.class)
@TestPropertySource(locations = "odilon.properties")
//@AutoConfigureMockMvc
public class QueryTest {
	
	static private Logger logger = Logger.getLogger(QueryTest.class.getName());
	
	@Autowired
	ObjectStorageService ds;

	@SuppressWarnings("unused")
	@Autowired
	private VirtualFileSystemService virtualFileSystemService;
	
	
	public QueryTest() {
		logger.debug("Start " + this.getClass().getName());
	}
	

	/**
	public QueryTest(ObjectStorageService ds, VirtualFileSystemService virtualFileSystemService) {
		this.ds=ds;
		this.virtualFileSystemService=virtualFileSystemService;
	}
	**/
	
	
	@Test
	public void executeTest() {
		
		logger.debug("hola");
		
	}

}
