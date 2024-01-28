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


@Component
@Scope("prototype")
@JsonTypeName("test")
public class TestServiceRequest extends AbstractServiceRequest implements StandardServiceRequest {
			
	private static final long serialVersionUID = 1L;
	
	static private Logger logger =	Logger.getLogger(TestServiceRequest.class.getName());
	
	private boolean isSuccess = false;
	
	public TestServiceRequest() {
	}
	
/**
 * {link ServiceRequestExecutor} closes the Request
 */
	
	@Override
	public void execute() {
		
		try {
			logger.debug("Testing " + getName());
		} catch (Exception e) {
			isSuccess=false;
			logger.error(e);
		} finally {
			isSuccess=true;
			setStatus(ServiceRequestStatus.COMPLETED);
		}
	}
	
	@Override
	public void stop() {
		setStatus(ServiceRequestStatus.STOPPED);
	}

	@Override
	public boolean isSuccess() {
		return isSuccess;
	}

	public boolean isObjectOperation() {
		return false;
	}
	
	@Override
	public String getUUID() {
		return "t" + getId().toString();
	}

}
