package io.odilon.scheduler;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import io.odilon.log.Logger;
import io.odilon.vfs.ODVFSOperation;
import io.odilon.vfs.model.VFSOperation;

/**
* <p>NOT USED YET</p>
* 
* @see {@link RAIDZeroUpdateObjectHandler}, {@link RAIDOneUpdateObjectHandler}
* 
* @author atolomei@novamens.com (Alejandro Tolomei) 
*/

@Component
@Scope("prototype")
@JsonTypeName("cleanUpWorkDir")
public class CleanUpWorkDirServiceRequest extends AbstractServiceRequest implements StandardServiceRequest {
		
static private Logger logger = Logger.getLogger(CleanUpWorkDirServiceRequest.class.getName());
	
	private static final long serialVersionUID = 1L;
	
	@JsonIgnore
	private boolean isSuccess = false;
	
	@JsonProperty("operation")
	private ODVFSOperation operation;
	

	protected CleanUpWorkDirServiceRequest() {
	}
	
	public CleanUpWorkDirServiceRequest(VFSOperation operation) {
		this.operation = (ODVFSOperation) operation;
	}
	
	/**
	 * <p>{@link ServiceRequestExecutor} closes the Request</p>
	 */
	@Override
	public void execute() {

		try {
			setStatus(ServiceRequestStatus.RUNNING);
			//VirtualFileSystemService  vfs = getApplicationContext().getBean(VirtualFileSystemService.class);
			//
			//
			// vfs.createVFSIODriver().cleanUpWorkDir(getVFSOperation());
			//
			// NOT USED
			//
			isSuccess = true;
			setStatus(ServiceRequestStatus.COMPLETED);
			
		} catch (Exception e) {
			 isSuccess=false;
			 logger.error(e);
		}
		//finally {
		//		getApplicationContext().getBean(SchedulerService.class).close(this);
		//}
	}

	@Override
	public boolean isObjectOperation() {
		return false;
	}

	
	@Override
	public void stop() {
		 isSuccess=true;
	}
	
	public VFSOperation getVFSOperation() {
		return operation;
	}
	
	@Override
	public boolean isSuccess() {
		return isSuccess;
	}

	@Override
	public String getUUID() {
		return operation.getUUID();
	}


}
