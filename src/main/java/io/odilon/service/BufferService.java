package io.odilon.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.cache.FileCacheService;
import io.odilon.log.Logger;
import io.odilon.model.BaseService;
import io.odilon.vfs.model.LockService;


@Service
public class BufferService extends BaseService {
				
	static private Logger logger = Logger.getLogger(FileCacheService.class.getName());
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
