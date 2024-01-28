package io.odilon.demo;

import java.util.List;

import io.odilon.client.ODClient;
import io.odilon.client.OdilonClient;
import io.odilon.client.error.ODClientException;
import io.odilon.model.Bucket;

public class SampleListBuckets {
			
	private String endpoint = "http://localhost";
	private int port = 9200;
	private String accessKey = "odilon";
	private String secretKey = "odilon";
	
	private OdilonClient client;
	
	
	 public SampleListBuckets() {
		 
	 }
	 
	 public void list() {
		 
		 
		 client = new ODClient(endpoint, port, accessKey, secretKey);
		 
		 String ping =client.ping();
		 
		 if (!ping.equals("ok")) {
			 System.out.println("ping error -> " + ping);			 
			 System.exit(1);
		 }
		 
		 try {
		
			 List<Bucket> listBuckets = client.listBuckets();
			 listBuckets.forEach( item -> System.out.println(item.toString()) );
		
		
		} catch (ODClientException e) {
			System.out.println(e.getClass().getName() + " " + e.getMessage());
		}
	 }
	 
	 
	 public static void main(String [] args) {
		 
		 System.out.println("Starting " + SampleListBuckets.class.getName() );
		 
		 SampleListBuckets listBuckets = new SampleListBuckets();
		 
		 listBuckets.list();
		 
		 System.out.println("done." );
	 }
	 

	 
}
