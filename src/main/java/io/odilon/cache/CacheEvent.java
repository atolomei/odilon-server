/*
 * Odilon Object Storage
 * (c) kbee 
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
package io.odilon.cache;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.log.Logger;
import io.odilon.service.BaseEvent;
import io.odilon.virtualFileSystem.Action;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * <p>
 * Spring events fired by the {@link JournalService} on <b>{@code commit}</b> or
 * <b>{@code cancel}</b> and listened by:
 * </p>
 * <ul>
 * <li>{@link FileCacheService} to invalidate File cache (used by RAID 6)</li>
 * <li>{@link ObjectMetadataCacheService} to invalidate {@link ObjectMetadata}
 * cache (used by RAID 0, RAID 1, RAID 6).</li>
 * </ul>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class CacheEvent extends BaseEvent {

	private static final long serialVersionUID = 1L;

	@JsonIgnore
	static private Logger logger = Logger.getLogger(CacheEvent.class.getName());

	public CacheEvent(VirtualFileSystemOperation operation, Action action) {
		super(operation, action);
	}
}
