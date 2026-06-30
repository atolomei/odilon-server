package io.odilon.virtualFileSystem.raid0;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.scheduler.AbstractServiceRequest;
import io.odilon.scheduler.ServiceRequest;
import io.odilon.virtualFileSystem.BaseRAIDHandler;
import io.odilon.virtualFileSystem.OdilonVirtualFileSystemOperation;
import io.odilon.virtualFileSystem.RAIDHandler;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.IODriver;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

public class RAIDZeroSchedulerHandler extends  BaseRAIDHandler implements RAIDHandler {

	@JsonIgnore
	private final RAIDZeroDriver driver;
	
	
	static private Logger logger = Logger.getLogger(RAIDZeroDriver.class.getName());
	 
	
	public RAIDZeroSchedulerHandler(RAIDZeroDriver driver) {
		this.driver = driver;
	}

	
	@Override
	public IODriver getDriver() {
		return driver;
	}

	@Override
	protected Drive getObjectMetadataReadDrive(ServerBucket bucket, String objectName) {
		return this.driver.getReadDrive(bucket, objectName);
	}


	public List<ServiceRequest> getSchedulerPendingRequests(String queueId) {

		List<ServiceRequest> list = new ArrayList<ServiceRequest>();

		Drive drive = getDriver().getDrivesEnabled().get(0);

		for (File file : drive.getSchedulerRequests(queueId)) {
			try {
				list.add((ServiceRequest) getObjectMapper().readValue(file, AbstractServiceRequest.class));
			} catch (Exception e) {
				// Log the error but DO NOT delete the file — deleting would permanently lose
				// pending replication/scheduler work that should be retried on next startup
				logger.error("Failed to deserialize ServiceRequest from file -> " + file.getAbsolutePath() + " | " + e.getClass().getName() + " | " + e.getMessage(), SharedConstant.NOT_THROWN);
			}
		}
		return list;
	}
	
	public void saveScheduler(ServiceRequest request, String queueId) {
		getDriver().getDrivesEnabled().get(0).saveScheduler(request, queueId);
	}
	
	
	public void removeScheduler(ServiceRequest request, String queueId) {
		getDriver().getDrivesEnabled().get(0).removeScheduler(request, queueId);
	}
	
	
}
