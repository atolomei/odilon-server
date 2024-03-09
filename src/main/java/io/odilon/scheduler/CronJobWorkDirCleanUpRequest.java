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

import java.io.File;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;

import io.odilon.log.Logger;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.VirtualFileSystemService;


/**
 * <p>removes work files older than {@link CronJobWorkDirCleanUpRequest#LAPSE_HOURS LAPSE_HOURS}.</p>
 */
@Component
@Scope("prototype")
@JsonTypeName("workDirCleanUp")
public class CronJobWorkDirCleanUpRequest extends CronJobRequest {
			
	static private Logger logger = io.odilon.log.Logger.getLogger(CronJobWorkDirCleanUpRequest.class.getName());

	private static final long serialVersionUID = 1L;
	private static final int LAPSE_HOURS = 3;
	
	@JsonIgnore
	private boolean isSuccess = false;
	
	@JsonIgnore
	private AtomicBoolean stop = new AtomicBoolean(false);

	
	protected CronJobWorkDirCleanUpRequest() {}
	
	public CronJobWorkDirCleanUpRequest(String exp) {
		super(exp);
	}
	
	@Override
	public void execute() {

		try {

			setStatus(ServiceRequestStatus.RUNNING);
			
			isSuccess = false;
			
			VirtualFileSystemService vfs = getApplicationContext().getBean(VirtualFileSystemService.class);
			
			OffsetDateTime now = OffsetDateTime.now();
			
			List<File> list = new ArrayList<File>();
			
			for (Drive drive: vfs.getMapDrivesAll().values()) {
				for (VFSBucket bucket: vfs.listAllBuckets()) {
					File bucketDir = new File (drive.getBucketWorkDirPath(bucket.getName()));
					if (bucketDir.exists()) {
						File files[] = bucketDir.listFiles();
		
						for (File fi:files) {
						
							if (stop.get())
								return;
							
							Instant instant = Instant.ofEpochMilli(fi.lastModified());
							OffsetDateTime modified = OffsetDateTime.ofInstant(instant, ZoneId.systemDefault());
							if(modified.plusHours(LAPSE_HOURS).isBefore(now)) {
								list.add(fi);									
							}
						}
					}
				}
			}
			
			if (stop.get())
				return;
			
			if (list.size()>0) {
				logger.debug("Removing from work dir -> " + String.valueOf(list.size()));
				list.forEach(item -> FileUtils.deleteQuietly(item));
			}
			
			isSuccess = true;

		} catch (Exception e) {
			logger.error(e);
		} finally {
			setStatus(ServiceRequestStatus.COMPLETED);
		}
	}
	@Override
	public boolean isSuccess() {
		return isSuccess;
	}
	
	@Override
	public void stop() {
		 stop.set(true);
	}

	@Override
	public String getUUID() {
		return "s"+ getId().toString();
	}
	


}
