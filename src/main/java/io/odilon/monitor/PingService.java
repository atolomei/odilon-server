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
package io.odilon.monitor;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.replication.ReplicationService;
import io.odilon.service.BaseService;
import io.odilon.service.ObjectStorageService;
import io.odilon.service.ServerSettings;

/**
 * <p>Monitor the status of the system and returns "ok" the system is normal or the problem found</p>
 * 
 * <pre>{@code # the following command should display the info page in the Linux console
 * 
 * # odilon default server (localhost) port (9234) and 
 * # credentials (accessKey: odilon, secretKey:odilon)
 * # these parameters can be edited in /config/odilon.properties
 *  
 *  sudo curl -u odilon:odilon localhost:9234/info
 * }
 * </pre>
 * 
 * <p>It is also called regularly by {@PingCronJobRequest}</p>


 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 */
@Service		
public class PingService extends BaseService implements ApplicationContextAware  {
	
	static private Logger logger = Logger.getLogger(PingService.class.getName());
	
	static final int defaultThreshold = Runtime.getRuntime().availableProcessors() + 2;
	
	@JsonIgnore
	private ApplicationContext applicationContext;
	
	@JsonIgnore
	private OffsetDateTime lastVaultReconnect = OffsetDateTime.now();
	
	public PingService() {}
	
	public synchronized List<String> pingList() {

		List<String> list = new ArrayList<String>();
		
		try {
				{
					String ping = pingCPULoad();
					if ( (ping==null) || (!ping.equals("ok")))  {
						list.add(ping==null?"cpu load null":ping);
					}
				}
				{
					String ping = getApplicationContext().getBean(ObjectStorageService.class).ping();
					if ( (ping==null) || (!ping.equals("ok")))  {
						list.add(ping==null?"null":ping);
					}
				}
				{
					ServerSettings serverSettings = getApplicationContext().getBean(ServerSettings.class);
					if (serverSettings.isStandByEnabled()) {
						ReplicationService replicationService = getApplicationContext().getBean(ReplicationService.class);
						String rping = replicationService.ping();
						if ( (rping==null) || (!rping.equals("ok")))  {
							list.add(rping==null?"replication ping null":rping);
						}
					}
				}
				
		} catch (Throwable e) {
			logger.error(e, SharedConstant.NOT_THROWN);
			list.add(e.getClass().getName()+" | " + e.getMessage());
		}
		return list;
		
	}
	public String pingString() {
		try {
		List<String> list = pingList();
		String str=list.stream().map((s) -> s).collect(Collectors.joining(" | "));
		return ((str==null || str.length()==0) ? "ok" :str);
		} catch (Exception e) {
			return (e.getClass().getName() + " | pingString");
		}
	}
	
	public ApplicationContext getApplicationContext()  {
		return this.applicationContext;
	}
	
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
	}

	private String pingCPULoad() {
		int processors = Runtime.getRuntime().availableProcessors();
		OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
		double load_average = os.getSystemLoadAverage();
		if (processors>0 && load_average>0.0) {
			Double percent = Double.valueOf (Double.valueOf(load_average) / Double.valueOf(processors));
			if (percent > defaultThreshold)
				return "CPU load -> "+ String.format("%6.2f", percent.doubleValue()*100.0)+ "%";
		}
		return "ok";
	}
}

