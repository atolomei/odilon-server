package io.odilon.client.unit;

import io.odilon.client.error.ODClientException;
import io.odilon.log.Logger;
import io.odilon.model.Bucket;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.list.Item;
import io.odilon.model.list.ResultSet;
import io.odilon.test.base.BaseTest;


/**
 * <p>A presigned URL is a way to grant temporary access to an Object, for example in an HTML webpage.
   It remains valid for a limited period of time which is specified when the URL is generated.
 * </p>
 * 
 *
 */
public class SamplePresignedUrl {

	private static final Logger logger = Logger.getLogger(TestObjectPutGet.class.getName());

	static int MAX = 100;
	
	private Bucket bucket;
	
	private String endpoint = "http://localhost";
	private int port = 9234;
	private String accessKey = "odilon";
	private String secretKey = "odilon";
    
	private OdilonClient client;
	

	public TestPresignedUrl() {
		try {
			
			this.client = new ODClient(endpoint, port, accessKey, secretKey);
			
		} catch (Exception e) {
			error(e.getClass().getName() +( e.getMessage()!=null ? (" | " + e.getMessage()) : ""));
		}
		
	}
	
	@Override
	public void executeTest() {

		try {
			if (getClient().listBuckets().isEmpty())
				error("must have at least 1 bucket");
			
			this.bucket = getClient().listBuckets().get(0);
			
			 ResultSet<Item<ObjectMetadata>> rs = getClient().listObjects(this.bucket.getName());
			 int counter = 0;
			 while (rs.hasNext() && counter++ < MAX) {
				 Item<ObjectMetadata> item = rs.next();
				 if (item.isOk()) {
					 	ObjectMetadata meta = item.getObject();
						
						/** by default the link lasts 7 days */
						logger.debug(meta.bucketName + " / " + meta.objectName + " (7 days) -> " + getClient().getPresignedObjectUrl(meta.bucketName, meta.objectName));	 
						
						/** url valid for 5 minutes */
						logger.debug(meta.bucketName + " / " + meta.objectName + " (5 min) -> " + getClient().getPresignedObjectUrl(meta.bucketName, meta.objectName, Optional<Integer>(Integer.valueOf(60*5)));	 
				 }
			 }
			 
		} catch (ODClientException e) {
			error(e);
		}
	}
	
	
	public OdilonClient getClient() { 
		return client;
	}
}









