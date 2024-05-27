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
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.service.ServerSettings;
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * 
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Component
@Scope("prototype")
@JsonTypeName("deleteBucketObjectPreviousVersion")
public class DeleteBucketObjectPreviousVersionServiceRequest extends AbstractServiceRequest implements StandardServiceRequest {
				
	static private Logger logger =	Logger.getLogger(DeleteBucketObjectPreviousVersionServiceRequest.class.getName());
	
	private static final long serialVersionUID = 1L;

	static AtomicBoolean instanceRunning = new AtomicBoolean(false);
	
	static final int PAGESIZE = 1000;
	
	private String bucketName;
	
	@JsonIgnore
	private long start_ms = 0;
	
	@JsonIgnore
	private boolean isSuccess = false;
	
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
	private int maxProcessingThread  = 1;

	
	@JsonIgnore
	private volatile ExecutorService executor;
	
	
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
		return true;
	}

	public String getBucketName() {
		return this.bucketName;
	}
	
	
	
	private void processBucket(String bname) {
		
		Integer pageSize = Integer.valueOf(PAGESIZE);
		Long offset = Long.valueOf(0);
		String agentId = null;
		boolean done = false;
		while (!done) {
			
			DataList<Item<ObjectMetadata>> data = getVirtualFileSystemService().listObjects(bname,
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
						if (item.isOk()) {
							process(item);
						}
						else
							this.notAvailable.getAndIncrement();
					
					} catch (Exception e) {
						logger.error(e, SharedConstant.NOT_THROWN);
					}
					return null;
				 });
			}
			
			try {
				executor.invokeAll(tasks, 20, TimeUnit.MINUTES);						
			} catch (InterruptedException e) {
				logger.error(e, SharedConstant.NOT_THROWN);
			}
			offset += Long.valueOf(Integer.valueOf(data.getList().size()).longValue());
			done = data.isEOD();
		}
	}
	
	/**
	 *
	 * 
	 */
	@Override
	public void execute() {

		try {
			
			setStatus(ServiceRequestStatus.RUNNING);
			
			if (instanceRunning.get()) {
				throw(new IllegalStateException("There is an instance already running -> " + this.getClass().getSimpleName()));
			}
			
			instanceRunning.set(true);
			
			logger.info("Starting -> " + getClass().getSimpleName() + " | b: " + (this.bucketName!=null? this.bucketName:"null"));
	
			this.start_ms = System.currentTimeMillis();
			this.counter = new AtomicLong(0);
			this.errors = new AtomicLong(0);
			this.notAvailable = new AtomicLong(0);
			this.checkOk = new AtomicLong(0);
			this.maxProcessingThread  = getServerSettings().getIntegrityCheckThreads();

			this.executor = Executors.newFixedThreadPool(this.maxProcessingThread);

			if (getBucketName()!=null)
				processBucket(getBucketName());
			else {
				for (VFSBucket bucket: getVirtualFileSystemService().listAllBuckets())
					processBucket(bucket.getName());
			}
			
			try {
				this.executor.shutdown();
				this.executor.awaitTermination(15, TimeUnit.MINUTES);
				
			} catch (InterruptedException e) {
			}
			
			this.isSuccess = true;
			setStatus(ServiceRequestStatus.COMPLETED);
			
		} catch (Exception e) {
			this.isSuccess=false;
			 logger.error(e, SharedConstant.NOT_THROWN);
	
		} finally {
			instanceRunning.set(false);
			logResults(logger);
		}
	}

	@Override
	public void stop() {
		 isSuccess=false;
	}

	private void logResults(Logger lg) {
		lg.info("Threads: " + String.valueOf(this.maxProcessingThread));
		lg.info("Total: " + String.valueOf(this.counter.get()));
		lg.info("Checked OK: " + String.valueOf(this.checkOk.get()));
		lg.info("Errors: " + String.valueOf(this.errors.get()));
		lg.info("Not Available: " + String.valueOf(this.notAvailable.get())); 
		lg.info("Duration: " + String.valueOf(Double.valueOf(System.currentTimeMillis() - start_ms) / Double.valueOf(1000)) + " secs");
		lg.info("---------");
	}
	
	private ServerSettings getServerSettings() {
		return getApplicationContext().getBean(ServerSettings.class);
	}

	private VirtualFileSystemService getVirtualFileSystemService() {
		return getApplicationContext().getBean(VirtualFileSystemService.class);
	}

	private void process(Item<ObjectMetadata> item) {
		try {
			getVirtualFileSystemService().deleteObjectAllPreviousVersions(item.getObject());
			this.checkOk.incrementAndGet();
		} catch (Exception e) {
			this.errors.getAndIncrement();
			logger.error(e, "Could not process -> " + item.getObject().bucketName + " - "+item.getObject().objectName + " " + SharedConstant.NOT_THROWN);
			
		}
	}

}
