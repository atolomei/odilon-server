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
package io.odilon.vfs.model;

import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;

import io.odilon.service.SystemService;


/**
 * <p>Lock Service for Object, FileCacheService, Bucket, ServerInfo. 
 * It maintains a Map of Object locks that is cleaned after the lock is released or regularly by a clean up process</p>
 *  
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public interface LockService extends SystemService {
	
	public ReadWriteLock getObjectLock(String bucketName, String objectName);
	public ReadWriteLock getBucketLock(String bucketName);
	
	public ReadWriteLock getServerLock();
	
	
	public ReadWriteLock getFileCacheLock(String bucketName, String objectName, Optional<Integer> version);
	
	
	
}
