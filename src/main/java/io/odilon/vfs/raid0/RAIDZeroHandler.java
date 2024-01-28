package io.odilon.vfs.raid0;

import io.odilon.model.RedundancyLevel;
import io.odilon.vfs.RAIDHandler;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.JournalService;
import io.odilon.vfs.model.LockService;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VirtualFileSystemService;

public abstract class RAIDZeroHandler implements RAIDHandler {

	
	private final RAIDZeroDriver driver;
	
	public RAIDZeroHandler(RAIDZeroDriver driver) {
		this.driver=driver;
	}

	public RAIDZeroDriver getDriver() {
		return this.driver;
	}
	
	public VirtualFileSystemService getVFS() {
		return this.driver.getVFS();
	}
	
	public abstract void rollbackJournal(VFSOperation op, boolean recoveryMode);

	public JournalService getJournalService() {
		return this.driver.getJournalService();
	}

	public LockService getLockService() {
		return this.driver.getLockService();
	}
	
	protected boolean isEncrypt() {
		return this.driver.isEncrypt();
	}
	
	public RedundancyLevel getRedundancyLevel() {
		return this.driver.getRedundancyLevel(); 
	}
	
	public Drive getWriteDrive(String bucketName, String objectName) {
		return this.driver.getWriteDrive(bucketName, objectName);
	}
	

}
