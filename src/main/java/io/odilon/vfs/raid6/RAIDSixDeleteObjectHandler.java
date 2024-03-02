package io.odilon.vfs.raid6;

import io.odilon.log.Logger;
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.VFSOperation;

public class RAIDSixDeleteObjectHandler extends RAIDSixHandler {

	private static Logger logger = Logger.getLogger(RAIDSixDeleteObjectHandler.class.getName());
	
	/**
	 * <p>Instances of this class are used
	 * internally by {@link RAIDSixDriver}</p>
	 * 
	 * @param driver
	 */
	protected RAIDSixDeleteObjectHandler(RAIDSixDriver driver) {
		super(driver);
	}
	
	@Override
	public void rollbackJournal(VFSOperation op, boolean recoveryMode) {
		throw new RuntimeException("not done");
		// TODO Auto-generated method stub
	}

	public void wipeAllPreviousVersions() {
		throw new RuntimeException("not done");
		// TODO Auto-generated method stub
	}

	public void deleteBucketAllPreviousVersions(VFSBucket bucket) {
		throw new RuntimeException("not done");
		// TODO Auto-generated method stub
	}

	public void deleteObjectAllPreviousVersions(VFSBucket bucket, String objectName) {
		throw new RuntimeException("not done");
		// TODO Auto-generated method stub
	}

	public void delete(VFSBucket bucket, String objectName) {
		throw new RuntimeException("not done");
		// TODO Auto-generated method stub
	}

	public void postObjectDeleteTransaction(String bucketName, String objectName, int headVersion) {
		throw new RuntimeException("not done");
		// TODO Auto-generated method stub
	}

	public void postObjectPreviousVersionDeleteAllTransaction(String bucketName, String objectName, int headVersion) {
		throw new RuntimeException("not done");
		// TODO Auto-generated method stub
	}

}
