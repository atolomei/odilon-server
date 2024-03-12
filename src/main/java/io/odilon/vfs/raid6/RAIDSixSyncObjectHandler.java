package io.odilon.vfs.raid6;

import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.util.Check;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VFSop;

@ThreadSafe
public class RAIDSixSyncObjectHandler extends RAIDSixHandler {
		
	private static Logger logger = Logger.getLogger(RAIDSixSyncObjectHandler.class.getName());
	
	protected RAIDSixSyncObjectHandler(RAIDSixDriver driver) {
		super(driver);
		}
	
	public void sync(String bucketName, String objectName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName is null");
		Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucketName);
		
		// TODO
		
		
		
	}
	
	
	@Override
	public void rollbackJournal(VFSOperation op, boolean recoveryMode) {
		Check.requireNonNullArgument(op, "op is null");
		Check.requireTrue(op.getOp()==VFSop.SYNC_OBJECT_NEW_DRIVE,VFSOperation.class.getName() + "can not be  ->  op: " + op.getOp().getName());
		
		getVFS().getObjectCacheService().remove(op.getBucketName(), op.getObjectName());
		
		switch (op.getOp()) {
					case SYNC_OBJECT_NEW_DRIVE: 
					{	
						execRollback(op, recoveryMode);
						break;
					}
					default: {
						break;	
					}
		}
	}
	
	/**
	 * 
	 * @param op
	 * @param recoveryMode
	 */
	private void execRollback(VFSOperation op, boolean recoveryMode) {
	
		boolean done = false;
		
		String bucketName = op.getBucketName();
		String objectName = op.getObjectName();
		
		try {
			getLockService().getObjectLock(bucketName, objectName).writeLock().lock();
			getLockService().getBucketLock(bucketName).readLock().lock();
			
			
			// TODO 
			
			done = true;
			
		} catch (InternalCriticalException e) {
			String msg = "Rollback -> " + op.toString();
			logger.error(msg);
			if (!recoveryMode)
				throw(e);

	
		} catch (Exception e) {
			String msg = "Rollback -> " + op.toString();
			logger.error(msg);
			
			if (!recoveryMode)
				throw new InternalCriticalException(e, msg);
		}
		finally {
				if (done || recoveryMode) {
					op.cancel();
				}
				
				getLockService().getBucketLock(bucketName).readLock().unlock();
				getLockService().getObjectLock(bucketName, objectName).writeLock().unlock();
				
		}
	}
	

}
