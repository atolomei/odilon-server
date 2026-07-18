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

import java.io.File;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.odilon.model.BaseObject;

/**
 * <p>
 * List of encoded blocks stored in File System. See the coding convention here
 * {@link ECDriver}
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class RData extends BaseObject {

	@JsonIgnore
	private List<File> encodedFiles;

	@JsonIgnore
	private File encodedFile;

	@JsonProperty("fileSize")
	private long fileSize;

	@JsonProperty("srcFileSize")
	private long srcFileSize;

	/** SHA 256 of the source (unecrypted) file */

	@JsonProperty("srcSha256")
	private String srcSha256;

	public RData() {
	}

	public long getFileSize() {
		return fileSize;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	public File getEncodedFile() {
		return encodedFile;
	}

	public void setEncodedFile(File encodedFile) {
		this.encodedFile = encodedFile;
	}

	@Override
	public String toJSON() {
		StringBuilder str = new StringBuilder();

		// if (getEncodedFile() != null) {
		// str.append("encodedFile ");
		// getEncodedShards().forEach(f -> str.append("\"" + f.getName() + "\" "));
		// str.append("]");
		// }
		str.append("\"fileSize\":" + String.valueOf(getFileSize()));
		return str.toString();
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(this.getClass().getSimpleName());
		str.append(toJSON());
		return str.toString();
	}

	public long getSrcFileSize() {
		return srcFileSize;
	}

	public void setSrcFileSize(long srcFileSize) {
		this.srcFileSize = srcFileSize;
	}

	public String getSrcSha256() {
		return srcSha256;
	}

	public void setSrcSha256(String srcSha256) {
		this.srcSha256 = srcSha256;
	}
}
