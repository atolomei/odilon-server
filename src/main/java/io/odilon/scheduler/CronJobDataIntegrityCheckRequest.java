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

import com.fasterxml.jackson.annotation.JsonTypeName;

import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.vfs.DataIntegrityChecker;



/**
 * <p>{@link CronJobRequest} that checks data integrity to prevent data corruption.</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Component
@Scope("prototype")
@JsonTypeName("dataIntegrity")
public class CronJobDataIntegrityCheckRequest extends CronJobRequest {
			
	static private Logger logger = Logger.getLogger(CronJobDataIntegrityCheckRequest.class.getName());

	private static final long serialVersionUID = 1L;
	
	/**
	 * 
	 */
	public CronJobDataIntegrityCheckRequest() {
		super();
	}

	/**
	 * 
	 */
	public CronJobDataIntegrityCheckRequest(String exp) {
		super(exp);
	}
	/**
	 *  <p>boolean value can be set to true to force scan all files</p>
	 */
	@Override
	public void execute() {

		try {
				setStatus(ServiceRequestStatus.RUNNING);
				boolean forceCheckAll = false;
				DataIntegrityChecker checker = getApplicationContext().getBean(DataIntegrityChecker.class, Boolean.valueOf(forceCheckAll));
				logger.debug("Started -> " + checker.toString());
			
			} catch (Exception e) {
				logger.error(e, SharedConstant.NOT_THROWN);
		
			} finally {
				setStatus(ServiceRequestStatus.COMPLETED);
				logger.debug("done");
			}
	}

	@Override
	public boolean isSuccess() {
		return true;
	}

	@Override
	public String getUUID() {
		return "s"+ getId().toString();
	}


	
}
