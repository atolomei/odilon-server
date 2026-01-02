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
package io.odilon.virtualFileSystem;

import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.SimpleDrive;

/**
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class ObjectDataPathBuilder extends PathBuilder {

	private final String objectName;
	private final ServerBucket bucket;
	private final ObjectPath path;

	public ObjectDataPathBuilder(SimpleDrive drive, ServerBucket bucket, String objectName) {

		this.objectName = objectName;
		this.bucket = bucket;
		path = new ObjectPath(drive, getBucket().getId(), getObjectName());
	}

	public String build() {
		return path.dataFilePath().toString();

	}

	private String getObjectName() {
		return objectName;
	}

	private ServerBucket getBucket() {
		return bucket;
	}

}
