package io.odilon.demo;

import java.util.Random;

import io.odilon.client.ODClient;
import io.odilon.client.OdilonClient;
import io.odilon.client.error.ODClientException;
import io.odilon.model.Bucket;

public class SampleBucketCreation {

	private String endpoint = "http://localhost";
	private int port = 9200;
	private String accessKey = "odilon";
	private String secretKey = "odilon";
	
	private OdilonClient client;
	
	
	 public SampleBucketCreation() {
		 
	 }
	 
	 public void create() {
		 
		 String bucketName = "bucket_" + randomString(4); 
		 
		 client = new ODClient(endpoint, port, accessKey, secretKey);
		 
		 String ping =client.ping();
		 
		 if (!ping.equals("ok")) {
			 System.out.println("ping error -> " + ping);			 
			 System.exit(1);
		 }
		 
		 try {
			if (client.existsBucket(bucketName)) 
				System.out.println("bucket already exists ->" + bucketName );
			else {

				client.createBucket(bucketName);			
				Bucket bucket = client.getBucket(bucketName);
				
				System.out.println( "Bucket created successfully ->  " + bucketName);
				System.out.println( bucket.toString());				
			}
		
		} catch (ODClientException e) {
			System.out.println(e.getClass().getName() + " " + e.getMessage());
		}
	 }
	 
	 
	 public static void main(String [] args) {
		 
		 System.out.println("Starting " + SampleBucketCreation.class.getName() );
		 
		 SampleBucketCreation bucketCreator = new SampleBucketCreation();
		 
		 bucketCreator.create();
		 
		 System.out.println("done." );
	 }
	 
	
	
	public String randomString(int size) {
			int leftLimit = 97; // letter 'a'
		    int rightLimit = 122; // letter 'z'
		    int targetStringLength =  size;
		    Random random = new Random();
		    String generatedString = random.ints(leftLimit, rightLimit + 1)
		      .limit(targetStringLength)
		      .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
		      .toString();
		    return generatedString;
		}	
}
