package io.odilon.demo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.odilon.client.ODClient;
import io.odilon.client.OdilonClient;
import io.odilon.client.error.ODClientException;
import io.odilon.model.Bucket;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.list.Item;
import io.odilon.model.list.ResultSet;

public class SampleListObjects {


	private String endpoint = "http://localhost";
	private int port = 9200;
	private String accessKey = "odilon";
	private String secretKey = "odilon";

	private OdilonClient client;
	
	public SampleListObjects() {}
	
	public void list() {
		
		client = new ODClient(endpoint, port, accessKey, secretKey);

		/** ping server. If the server is not online, exit */
		 String ping =client.ping();
		 if (!ping.equals("ok")) {
			 System.out.println("ping error -> " + ping);			 
			 System.exit(1);
		 }
		 
		 /** Get a Bucket. if there are none,  create one */
		 Bucket bucket = null;
		
		 try {

			 List<Bucket> listBuckets = client.listBuckets();
			 if (listBuckets.isEmpty())  
				 throw new RuntimeException("there are no buckets");
		 else
			 bucket = listBuckets.get(0);
		 } catch (ODClientException e) {
		    	System.out.println(String.valueOf(e.getHttpStatus())+ " " + e.getMessage() + " " + String.valueOf(e.getErrorCode()));
				System.exit(1);
		 }
		
		try {
		    	ResultSet<Item<ObjectMetadata>> resultSet = client.listObjects(bucket.getName(), Optional.empty(), Optional.empty());
		    	
		    	while (resultSet.hasNext()) {
		    		Item<ObjectMetadata> item = resultSet.next();
		    		if (item.isOk())
		    			System.out.println(" objectName:" + item.getObject().objectName +" | file: " + item.getObject().fileName);
		    		else
		    			System.out.println(item.getErrorString());
		    	}
		    } catch (ODClientException e) {
		    	System.out.println(String.valueOf(e.getHttpStatus())+ " " + e.getMessage() + " " + String.valueOf(e.getErrorCode()));
				System.exit(1);
			}
			
	}
		 
	
	public static void main(String [] args) {
		 
		 System.out.println("Starting " + SampleListObjects.class.getName() );
		 SampleListObjects lo = new SampleListObjects();
		 lo.list();
		 System.out.println("done." );
	 }
	
}
