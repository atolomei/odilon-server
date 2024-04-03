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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;


/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public interface SimpleDrive extends Drive {
	
	public InputStream getObjectInputStream(String bucketName, String objectName);
	public File putObjectStream(String bucketName, String objectName, InputStream stream) throws IOException;
	public void putObjectDataFile(String bucketName, String objectName, File objectFile) throws IOException;
	public void putObjectDataVersionFile(String bucketName, String objectName, int version, File objectFile) throws IOException;

	public void deleteObjectMetadata(String bucketName, String objectName);
	
	public File getObjectDataFile(String bucketName, String objectName);
	public File getObjectDataVersionFile(String bucketName, String objectName, int version);
	
	public String getObjectDataFilePath			(String bucketName, String objectName);
	public String getObjectDataVersionFilePath	(String bucketName, String objectName, int version);
}

