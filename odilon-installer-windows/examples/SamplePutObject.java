package io.odilon.demo;

import java.io.File;
import java.util.List;
import java.util.Random;

import io.odilon.client.ODClient;
import io.odilon.client.OdilonClient;
import io.odilon.client.error.ODClientException;
import io.odilon.model.Bucket;
import io.odilon.model.ObjectMetadata;


public class SamplePutObject {

	static final long MAX_LENGTH = 5 * 1000 * 1000; // 5MB
	
	private String endpoint = "http://localhost";
	private int port = 9200;
	private String accessKey = "odilon";
	private String secretKey = "odilon";
	
	private OdilonClient client;
		
	
	public SamplePutObject() {
		 
	}
	
	public void upload() {
		 
		 String dirName;
		
		 if (isWindows())
			dirName = "c:" + File.separator + "temp";
		else
			dirName = "/tmp";

		 final File dir = new File(dirName);	 
		  
		 if  ((!dir.exists()) || (!dir.isDirectory()))  
				throw new RuntimeException("Dir not exists or the File is not Dir -> " + dirName + " | Please create directory and put at least 1 pdf or image for this example to upload" );
		 
		 if (dir.list()==null || dir.list().length==0)
			 throw new RuntimeException("Sample Dir is empty or not authorized  -> " + dirName );
		 
		 
		 /** create Odilon Client */
		 System.out.println("create OdilonClient");
		 System.out.println("OdilonClient is an interface, the actual class is ODClient");
		 
		 client = new ODClient(endpoint, port, accessKey, secretKey);
		 
		 /** ping server. If the server is not online, exit */
		 String ping =client.ping();
		 if (!ping.equals("ok")) {
			 System.out.println("ping error -> " + ping);			 
			 System.exit(1);
		 }
		 System.out.println("-----------");
		 
		 /** Get a Bucket. if there are none,  create one */
		 Bucket bucket = null;
		 try {
			 List<Bucket> listBuckets = client.listBuckets();
			 if (listBuckets.isEmpty()) 
				 bucket=createBucket();
			 else
				 bucket = listBuckets.get(0);
			 
		} catch (ODClientException e) {
			System.out.println(e.getClass().getName() + " " + e.getMessage());
			 System.exit(1);
		}
		 
		 /**  upload file **/
		 String objectName = null;
		 
		 System.out.println("Upload file");
		 
		 for (File file:dir.listFiles()) {

			 if (isElegible(file)){
				 try {
			
					 objectName = file.getName() + "-" + String.valueOf(Double.valueOf((Math.abs(Math.random()*100000))).intValue());
					 client.putObject(bucket.getName(), objectName, file);
					 System.out.println("uploaded ->  bucketName: " + bucket.getName() + " | objectName: " + objectName + " | file: " + file.getAbsolutePath());
					 break;
											
					} catch (ODClientException e) {
						System.out.println(String.valueOf(e.getHttpStatus())+ " " + e.getMessage() + " " + String.valueOf(e.getErrorCode()));
						System.exit(1);
					}
			 }
		 }
		 System.out.println("-----------");		 
		 
		 
		 /**  download ObjectMetadata **/
		 System.out.println("Download ObjectMetadata");
		 
		 if (objectName!=null) {
			 try {
				 ObjectMetadata meta = client.getObjectMetadata(bucket.getName(), objectName);
				 System.out.println("ObjectMetadata -> " + meta.toString());
				 
			 } catch (ODClientException e) {
					System.out.println(String.valueOf(e.getHttpStatus())+ " " + e.getMessage() + " " + String.valueOf(e.getErrorCode()));
					System.exit(1);
			 } 
		 }
		 System.out.println("-----------");
	 }
	 

	
	 private Bucket createBucket() {
		 
		 System.out.println("create bucket");
		 String bucketName = "bucket_" + randomString(4);
		 try {
			client.createBucket(bucketName);
			System.out.println( "Bucket created successfully ->  " + bucketName);
			return client.getBucket(bucketName);
			
		} catch (ODClientException e) {
			System.out.println(String.valueOf(e.getHttpStatus())+ " " + e.getMessage() + " " + String.valueOf(e.getErrorCode()));
			System.exit(1);
		}
		 
		return null;
	}

	private boolean isElegible(File file) {
		 
		 if (file.isDirectory())
			 return false;
		 
		 if (file.length()> MAX_LENGTH)
			 return false;
		 
		 String name = file.getName();
		 
		 if (name.toLowerCase().matches("^.*\\.(png|jpg|jpeg|gif|bmp|heic|webp|tiff)$"))
			 return true;

		 if (name.toLowerCase().matches("^.*\\.(pdf|doc|docx|rtf|xlx|xlsx|ppt|pptx|xlsm)$"))
			 return true;
		 
		return true;
		
	}
	
	private String randomString(int size) {
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

	 
	 
	public static void main(String [] args) {
		 
		 System.out.println("Starting " + SamplePutObject.class.getName() );
		 SamplePutObject put = new SamplePutObject();
		 put.upload();
		 System.out.println("done." );
	 }
	
	private static boolean isWindows() {
		if  (System.getenv("OS")!=null && System.getenv("OS").toLowerCase().contains("windows"))
			return true;
		return false;
}

}
