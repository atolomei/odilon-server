package io.odilon.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;

import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ServiceStatus;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.service.ServerSettings;
import io.odilon.service.SystemService;
import io.odilon.util.Check;
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.VirtualFileSystemService;


@Component
@Scope("prototype")
@JsonTypeName("deleteBucketObjectPreviousVersion")
public class DeleteBucketObjectPreviousVersionServiceRequest extends AbstractServiceRequest implements StandardServiceRequest {
				
	static private Logger logger =	Logger.getLogger(DeleteBucketObjectPreviousVersionServiceRequest.class.getName());
	
	private static final long serialVersionUID = 1L;
	
	private String bucketName;

	static AtomicBoolean instanceRunning = new AtomicBoolean(false);
	
	static final double KB = 1024.0;
	static final double MB = 1024.0 * KB;
	static final double GB = 1024.0 * MB;
	
	
	@JsonIgnore
	long start_ms = 0;
	
	@JsonIgnore
	private boolean isSuccess = false;
	
	@JsonIgnore
	private int maxProcessingThread  = 1;
	
	
	public DeleteBucketObjectPreviousVersionServiceRequest() {
	}
	
	public DeleteBucketObjectPreviousVersionServiceRequest(String bucketName) {
		this.bucketName=bucketName;
	}

	@Override
	public boolean isSuccess() {
		return isSuccess;
	}

	@Override
	public String getUUID() {
		return "s:" + this.getClass().getSimpleName();
	}

	@Override
	public boolean isObjectOperation() {
		return false;
	}

	public String getBucketName() {
		return this.bucketName;
	}
	
	
	
	@JsonIgnore
	private AtomicLong checkOk = new AtomicLong(0);
	
	@JsonIgnore
	private AtomicLong counter = new AtomicLong(0);
	

	@JsonIgnore
	private AtomicLong totalBytes = new AtomicLong(0);
	
	@JsonIgnore
	private AtomicLong errors = new AtomicLong(0);
	
	@JsonIgnore
	private AtomicLong notAvailable = new AtomicLong(0);
	
	
	@JsonIgnore
	private volatile ExecutorService executor;
	
	
	static final int PAGESIZE = 1000;
	
	
	
	private void processBucket(String bname) {
		
		Integer pageSize = Integer.valueOf(PAGESIZE);
		Long offset = Long.valueOf(0);
		String agentId = null;
		boolean done = false;
		while (!done) {
			
			DataList<Item<ObjectMetadata>> data = getVirtualFileSystemService().listObjects(
					bname, 
					Optional.of(offset),
					Optional.ofNullable(pageSize),
					Optional.empty(),
					Optional.ofNullable(agentId)); 

			if (agentId==null)
				agentId = data.getAgentId();

			List<Callable<Object>> tasks = new ArrayList<>(data.getList().size());
			
			for (Item<ObjectMetadata> item: data.getList()) {
				
				tasks.add(() -> {
								
					try {
						this.counter.getAndIncrement();
						if (item.isOk())
							process(item);
						else
							this.notAvailable.getAndIncrement();
					
					} catch (Exception e) {
						logger.error(e);
					}
					return null;
				 });
			}
			
			try {
				executor.invokeAll(tasks, 20, TimeUnit.MINUTES);						
			} catch (InterruptedException e) {
				logger.error(e);
			}
			offset += Long.valueOf(Integer.valueOf(data.getList().size()).longValue());
			done = data.isEOD();
		}
	}
	
	
	
	@Override
	public void execute() {

		try {
			
			setStatus(ServiceRequestStatus.RUNNING);
			
			if (instanceRunning.get()) {
				throw(new IllegalStateException("There is an instance already running -> " + this.getClass().getSimpleName()));
			}
			
			instanceRunning.set(true);
			
			logger.info("Starting -> " + getClass().getSimpleName());
	
			this.start_ms = System.currentTimeMillis();
			this.counter = new AtomicLong(0);
			this.errors = new AtomicLong(0);
			this.notAvailable = new AtomicLong(0);
			this.checkOk = new AtomicLong(0);
			this.maxProcessingThread  = getServerSettings().getIntegrityCheckThreads();

			executor = Executors.newFixedThreadPool(this.maxProcessingThread);

			if (getBucketName()!=null)
				processBucket(getBucketName());
			else {
				for (VFSBucket bucket: getVirtualFileSystemService().listAllBuckets())
					processBucket(bucket.getName());
			}
			
			try {
				executor.shutdown();
				executor.awaitTermination(15, TimeUnit.MINUTES);
				
			} catch (InterruptedException e) {
			}
			
			isSuccess = true;
			setStatus(ServiceRequestStatus.COMPLETED);
			
		} catch (Exception e) {
			 isSuccess=false;
			 logger.error(e);
	
		} finally {
			
			instanceRunning.set(false);
			
			logResults(logger);
		}
	}

		
	private void process(Item<ObjectMetadata> item) {
		try {
			
			
			ObjectMetadata meta = item.getObject();

			getVirtualFileSystemService().deleteObjectAllPreviousVersions(meta.bucketName, meta.objectName);
			
			//this.totalBytes.addAndGet(item.getObject().length);
			this.checkOk.incrementAndGet();
			
		} catch (Exception e) {
			this.errors.getAndIncrement();
			logger.error(e);
			logger.error("Could not process -> " + item.getObject().bucketName + " - "+item.getObject().objectName);
			
		}
	}

	
	
	private ServerSettings getServerSettings() {
		return getApplicationContext().getBean(ServerSettings.class);
	}

	private VirtualFileSystemService getVirtualFileSystemService() {
		return getApplicationContext().getBean(VirtualFileSystemService.class);
	}

	@Override
	public void stop() {
		 isSuccess=false;
	}

	private void logResults(Logger lg) {
		lg.info("Threads: " + String.valueOf(this.maxProcessingThread));
		lg.info("Total: " + String.valueOf(this.counter.get()));
		
		//lg.info("Total Size: " + String.format("%14.4f", Double.valueOf(totalBytes.get()).doubleValue() / GB).trim() + " GB");
		
		lg.info("Checked OK: " + String.valueOf(this.checkOk.get()));
		lg.info("Errors: " + String.valueOf(this.errors.get()));
		lg.info("Not Available: " + String.valueOf(this.notAvailable.get())); 
		lg.info("Duration: " + String.valueOf(Double.valueOf(System.currentTimeMillis() - start_ms) / Double.valueOf(1000)) + " secs");
		lg.info("---------");
		
	}

}
