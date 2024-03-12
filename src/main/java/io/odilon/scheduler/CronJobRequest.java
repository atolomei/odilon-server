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

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.errors.InternalCriticalException;

/**
 * 
 * <p>{@link ServiceRequest} that executes regularly based on a {@link CronExpressionJ8}</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Component
@Scope("prototype")
public abstract class CronJobRequest extends AbstractServiceRequest {
			
	static private Logger logger = LogManager.getLogger(CronJobRequest.class.getName());
	
	private static final long serialVersionUID = 1L;

	@JsonIgnore
	private ZonedDateTime ztime = null;
	
	@JsonIgnore
	private boolean executeOldTriggers;
	
	@JsonIgnore
	private boolean is_user_request = false;
	
	@JsonIgnore
	private boolean enabled = true;
	
	private CronExpressionJ8 cron_expression;
	
	
	public CronJobRequest() {
		super();
		setTimeZone(TimeZone.getDefault().getID());
	}
	
	public CronJobRequest(String exp) {
		super();
		this.cron_expression = new CronExpressionJ8(exp,true);
		setTimeZone(TimeZone.getDefault().getID());
	}
	
	@Override
	public boolean isCronJob() {
		return true;
	}
	
	public boolean isEnabled() {
		return this.enabled;
	}
	
	public void setEnabled(boolean b) {
		this.enabled=b;
	}
	
	public ZonedDateTime getTime() {
		if (this.ztime==null) 
			this.ztime = getCronExpression().nextTimeAfter((getTimeZone()==null) ? ZonedDateTime.now() : ZonedDateTime.now(ZoneId.of(getTimeZone())));
		return this.ztime;
	}
	
	public void setTime(ZonedDateTime time) {
		this.ztime = time;
	}
	
	public ZonedDateTime getNextTime() {
		return getCronExpression().nextTimeAfter(getTime());
	}
	
	public CronExpressionJ8 getCronExpression() {
		return this.cron_expression;
	}
	
	public void setCronExpression(CronExpressionJ8 expression) {
		this.cron_expression = expression;
	}
	
	
	@Override
	public void stop() {
	}
	
	public void setUserRequest(boolean b) {
		 this.is_user_request = b;
	}
	
	public boolean isUserRequest() {
		return this.is_user_request;
	}
	
	public void onClone(CronJobRequest clone) {
		clone.setCronExpression(getCronExpression());
		clone.setName(getName());
		clone.setDescription(getDescription());
		clone.setUserRequest(isUserRequest());
		clone.setTime(getCronExpression().nextTimeAfter(getTime()));
		clone.setExecuteOldTriggers(getExecuteOldTriggers());
		clone.setEnabled(isEnabled());
		clone.setExecuteAfter(getExecuteAfter());
		clone.setTimeZone(getTimeZone());
		Map<String, String> pa=getParameters();
		if (pa!=null) {
			for(Entry<String, String> e: pa.entrySet()) 
				pa.put(e.getKey().toString(), e.getValue());
			clone.setParameters(pa);
		}
	}
	
	public CronJobRequest clone() {
		try {
			CronJobRequest clone = getApplicationContext().getBean(this.getClass(), getCronExpression().getExpression());
			onClone(clone);
			return clone;
		}
		catch (Exception e) {
			logger.error(e);
			throw new InternalCriticalException(e);
		}
	}
	
	public boolean getExecuteOldTriggers() {
		return executeOldTriggers;
	}

	public void setExecuteOldTriggers(boolean executeOldTriggers) {
		this.executeOldTriggers = executeOldTriggers;
	}

	


}
