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
package io.odilon.file;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.util.Check;
import io.odilon.util.DateTimeUtil;
import io.odilon.virtualFileSystem.model.Drive;

/**
 * 
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class ParallelFileCoypAgent extends FileCopyAgent {

	static private Logger logger = Logger.getLogger(ParallelFileCoypAgent.class.getName());
	
	@JsonIgnore
	final byte[][] source;
	
	@JsonIgnore
	final List<File> destination;
	
	@JsonIgnore
	final List<Drive> drives;
	
	@JsonIgnore
	private ExecutorService executor;
	
	@JsonIgnore
	private OffsetDateTime start;
	
	@JsonIgnore
	private OffsetDateTime end;
	
	public ParallelFileCoypAgent(List<Drive> drives, byte[][] source, List<File> destination) {
		
		Check.requireNonNull(source);
		Check.requireNonNull(destination);
		Check.requireTrue(source.length==destination.size(), "source and destination must have the same number of elements. "
				+ "source -> " + String.valueOf(source.length) + " | destination -> " + String.valueOf(destination.size()));
		
		this.drives=drives;
		this.source=source;
		this.destination=destination;
	}
	
	@Override
	public long durationMillisecs() {
		if (this.start==null || this.end==null)
			return -1;
		return  DateTimeUtil.dateTimeDifference(start, end, ChronoUnit.MILLIS);
	}
	
	@Override
	public boolean execute() {
	
		try {

			this.start = OffsetDateTime.now();
		
			int size = this.destination.size();
			
			/** Thread pool */
			this.executor = Executors.newFixedThreadPool(size);
			
			List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>(size);
			
			for (int index=0; index<size; index++) {
				
				final int val=index;

				tasks.add(() -> {
					try {
						File outputFile = this.destination.get(val);
	   					try  (OutputStream out = new BufferedOutputStream(new FileOutputStream(outputFile))) {
	    						out.write(this.source[val]);
	   			        } catch (FileNotFoundException e) {
	    						throw new InternalCriticalException(e, "f: " + outputFile.getName());
	   					} catch (IOException e) {
	   						throw new InternalCriticalException(e, "f: " + outputFile.getName());
	   					}
						return Boolean.valueOf(true);
						
					} catch (Exception e) {
						logger.error(e, SharedConstant.NOT_THROWN);
						return Boolean.valueOf(false);
					} finally {
						
					}
				});
			}
			/** process buffer in parallel */
			try {
				 List <Future<Boolean>>  future = this.executor.invokeAll(tasks, 10, TimeUnit.MINUTES);
				 Iterator<Future<Boolean>> it = future.iterator();
				 while (it.hasNext()) {
					if (!it.next().get())
						return false;
				 }
				 
			} catch (InterruptedException e) {
				logger.error(e, SharedConstant.NOT_THROWN);
			}
			
			return true;
			
		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
			return false;
			
		} finally {
			this.end = OffsetDateTime.now();
			//logger.debug("Duration: " +  DateTimeUtil.timeElapsed(this.start, this.end));
		}
	}
}

