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
import io.odilon.monitor.PingService;

/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Component
@Scope("prototype")
@JsonTypeName("ping")
public class PingCronJobRequest extends CronJobRequest {
			
	static private Logger logger = Logger.getLogger(PingCronJobRequest.class.getName());

	private static final long serialVersionUID = 1L;
	
	public PingCronJobRequest() {
	}
	
	public PingCronJobRequest (String exp) {
		super(exp);
	}
	
	@Override
	public void execute() {
		try {
			setStatus(ServiceRequestStatus.RUNNING);
			String ping = getApplicationContext().getBean(PingService.class).pingString();
			logger.debug("ping -> " + ping);
			setStatus(ServiceRequestStatus.COMPLETED);
			
		} catch (Throwable e) {
			setStatus(ServiceRequestStatus.ERROR);
			logger.error(e, SharedConstant.NOT_THROWN);
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
