/*
 * Odilon Object Storage
 * (C) Novamens 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.odilon.scheduler;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ServerConstant;
import io.odilon.vfs.model.VFSop;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * <p>ServiceRequest executed Async after a 
 * {@code VFSop.DELETE_OBJECT} or {@code VFSop.DELETE_OBJECT_PREVIOUS_VERSIONS}</p>  
 * 
 * <b>VFSop.DELETE_OBJECT</b>
 * <p>Cleans up all previous versions of an {@link VFSObject} (ObjectMetadata and Data).<br/>
 * Delete backup directory. <br/>
 * This request is executed Async after the delete transaction commited.</p>
 *	 
 * <b>VFSop.DELETE_OBJECT_PREVIOUS_VERSIONS</b>
 * <p>Cleans up all previous versions of an {@link VFSObject} (ObjectMetadata and Data), but keeps the head version.<br/>
 * Delete backup directory. <br/>
 * This request is executed Async after the delete transaction commited.</p>
 *
 * <b>RETRIES</b>
 * <p>if the request can not complete due to serious system issue, the request is discarded after 5 attemps. 
 * The clean up process will be executed after next system startup</p>
 * 
 * @see {@link RAIDZeroDeleteObjectHandler}, {@link RAIDOneDeleteObjectHandler} 
 * @author atolomei@novamens.com (Alejandro Tolomei)
  */
@Component
@Scope("prototype")
@JsonTypeName("afterDeleteObject")
public class AfterDeleteObjectServiceRequest extends AbstractServiceRequest implements StandardServiceRequest {

	static private Logger logger = Logger.getLogger(AfterDeleteObjectServiceRequest.class.getName());
	
	private static final long serialVersionUID = 1L;
 
	@JsonProperty("meta")
	ObjectMetadata meta;
	
	@JsonProperty("headVersion")
	int headVersion=0;

	@JsonProperty("vfsop")
	VFSop vfsop;
	
	@JsonIgnore
	private boolean isSuccess = false;
	
	/**
	 * <p>created by the RAIDZeroDriver</p>
	 */
	protected AfterDeleteObjectServiceRequest() {
	}
	
	public AfterDeleteObjectServiceRequest(VFSop vfsop, ObjectMetadata meta, int headVersion) {
		
		this.vfsop=vfsop;
		this.meta=meta;
		this.headVersion=headVersion;
	}
	
	@Override
	public boolean isSuccess() {
		return isSuccess;
	}

	/**
	 * <p>{@link ServiceRequestExecutor} closes the Request</p>
	 */
	@Override
	public void execute() {
		try {
			setStatus(ServiceRequestStatus.RUNNING);
			clean();
			isSuccess=true;
			setStatus(ServiceRequestStatus.COMPLETED);
			
		} catch (Exception e) {
			logger.error(e, ServerConstant.NOT_THROWN);
			isSuccess=false;
			setStatus(ServiceRequestStatus.ERROR);
		}
	}

	@Override
	public String getUUID() {
		if (meta==null)
			return "null";
		return  ((meta.bucketName!=null) ? meta.bucketName :"null" ) + ":" + 
				((meta.objectName!=null) ? meta.objectName :"null" );
	}
	
	@Override
	public boolean isObjectOperation() {
		return true;
	}
	
	@Override
	public void stop() {
		 isSuccess=true;
	}

	private void clean() {
			
		VirtualFileSystemService vfs = getApplicationContext().getBean(VirtualFileSystemService.class);
			
		if (this.vfsop==VFSop.DELETE_OBJECT)
				vfs.createVFSIODriver().postObjectDeleteTransaction(meta, headVersion);
			
		else if (this.vfsop==VFSop.DELETE_OBJECT_PREVIOUS_VERSIONS) 
				vfs.createVFSIODriver().postObjectPreviousVersionDeleteAllTransaction(meta, headVersion);
		else
			logger.error("Invalid " + VFSop.class.getName() + " -> " + this.vfsop.getName(), ServerConstant.NOT_THROWN);
	}

}
