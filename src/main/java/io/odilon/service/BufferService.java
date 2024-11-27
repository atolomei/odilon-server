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
package io.odilon.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.cache.FileCacheService;
import io.odilon.log.Logger;
import io.odilon.virtualFileSystem.model.LockService;

/**
 * 
 * NOT USED 
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 * 
 */
@Service
public class BufferService extends BaseService {
				
	@SuppressWarnings("unused")
	static private Logger logger = Logger.getLogger(FileCacheService.class.getName());
	@SuppressWarnings("unused")
	static private Logger startuplogger = Logger.getLogger("StartupLogger");
	
	@JsonIgnore
	@Autowired
	private final ServerSettings serverSettings;
	
	public BufferService(ServerSettings serverSettings, LockService vfsLockService) {
		this.serverSettings=serverSettings;
	}
	
	
	/**
	 *  byte [] getEncodingSourceBuffer
	 *  byte [] getEncodingRSBuffer
	 *  
	 *  byte [] getDecodingSrcBuffer
	 *  byte [] getDecodingRSBuffer
	 *   
	 *  Map<buffers>
	 *  
	 *  FIFO Available put, get 
	 *  InUse MAP
	 *   
	 */

	
	
}
