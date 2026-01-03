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
package io.odilon.api;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.http.MediaType;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.odilon.model.BaseObject;
import io.odilon.model.ObjectMetadata;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.service.ObjectStorageService;
import io.odilon.traffic.TrafficControlService;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * Base class for all API Controllers
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public abstract class BaseApiController extends BaseObject implements ApplicationContextAware {

	@JsonIgnore
	private ApplicationContext applicationContext;

	@JsonIgnore
	@Autowired
	private SystemMonitorService monitoringService;

	@JsonIgnore
	@Autowired
	protected ObjectStorageService objectStorageService;

	@JsonIgnore
	@Autowired
	protected VirtualFileSystemService virtualFileSystemService;

	@JsonIgnore
	@Autowired
	TrafficControlService trafficControlService;

	@Autowired
	public BaseApiController(ObjectStorageService objectStorageService, VirtualFileSystemService virtualFileSystemService, SystemMonitorService monitoringService) {

		this(objectStorageService, virtualFileSystemService, monitoringService, null);
	}

	@Autowired
	public BaseApiController(ObjectStorageService objectStorageService, VirtualFileSystemService virtualFileSystemService, SystemMonitorService monitoringService, TrafficControlService trafficControlService) {

		this.objectStorageService = objectStorageService;
		this.virtualFileSystemService = virtualFileSystemService;
		this.monitoringService = monitoringService;
	}

	@PostConstruct
	protected void init() {
	}

	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}

	public TrafficControlService getTrafficControlService() {
		return trafficControlService;
	}

	public VirtualFileSystemService getVirtualFileSystemService() {
		return virtualFileSystemService;
	}

	public void setVirtualFileSystemService(VirtualFileSystemService virtualFileSystemService) {
		this.virtualFileSystemService = virtualFileSystemService;
	}

	public SystemMonitorService getSystemMonitorService() {
		return this.monitoringService;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

	public ObjectStorageService getObjectStorageService() {
		return objectStorageService;
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(toJSON());
		return str.toString();
	}

	protected long getSrcFileLength(ObjectMetadata objectMetadata) {

		long fileLength = 0;

		if (objectMetadata.getSourceLength() > 0) {
			fileLength = objectMetadata.getSourceLength();
			return fileLength;
		}

		if (!objectMetadata.isEncrypt()) {
			fileLength = objectMetadata.getLength();
			return fileLength;
		}
		fileLength = -1;
		return fileLength;
	}

	protected MediaType estimateContentType(String f_name) {

		if (f_name == null)
			return MediaType.valueOf("application/octet-stream");

		if (isPdf(f_name))
			return MediaType.valueOf("application/pdf");

		if (isAudio(f_name))
			return MediaType.valueOf("audio/mpeg");

		if (isVideo(f_name))
			return MediaType.valueOf("video/mpeg");

		if (isJpeg(f_name))
			return MediaType.valueOf("image/jpeg");

		if (isPng(f_name))
			return MediaType.valueOf("image/png");

		if (isSvg(f_name))
			return MediaType.valueOf("image/svg+xml");

		if (isGif(f_name))
			return MediaType.valueOf("image/gif");

		if (isWebp(f_name))
			return MediaType.valueOf("image/webp");

		if (isExcel(f_name))
			return MediaType.valueOf("application/vnd.ms-excel");

		if (isWord(f_name))
			return MediaType.valueOf("application/msword");

		if (isPowerPoint(f_name))
			return MediaType.valueOf("application/vnd.ms-powerpoint");

		return MediaType.valueOf("application/octet-stream");
	}

	protected void mark() {
		getSystemMonitorService().getAllAPICallMeter().mark();
	}

	protected String getMessage(Throwable e) {

		if (e == null)
			return "null";

		StringBuilder str = new StringBuilder();

		str.append(e.getClass().getName());

		if (e.getMessage() != null)
			str.append(" | " + e.getMessage());

		if (e.getCause() != null)
			str.append(" | " + e.getCause());

		return str.toString();
	}

	static public boolean isPowerPoint(String name) {
		return name.toLowerCase().matches("^.*\\.(ppt|pptx)$");
	}

	static public boolean isWord(String name) {
		return name.toLowerCase().matches("^.*\\.(doc|docx|rtf)$");
	}

	static public boolean isVideo(String filename) {
		return (filename.toLowerCase().matches("^.*\\.(mp4|flv|aac|ogg|wmv|3gp|avi|swf|svi|wtv|fla|mpeg|mpg|mov|m4v)$"));
	}

	static public boolean isAudio(String filename) {
		return filename.toLowerCase().matches("^.*\\.(mp3|wav|ogga|ogg|aac|m4a|m4a|aif|wma)$");
	}

	static public boolean isExcel(String name) {
		return name.toLowerCase().matches("^.*\\.(xls|xlsx|xlsm)$");
	}

	static public boolean isJpeg(String string) {
		return string.toLowerCase().matches("^.*\\.(jpg|jpeg)$");
	}

	static public boolean isPdf(String string) {
		return string.toLowerCase().matches("^.*\\.(pdf)$");
	}

	static public boolean isGif(String string) {
		return string.toLowerCase().matches("^.*\\.(gif)$");
	}

	static public boolean isWebp(String string) {
		return string.toLowerCase().matches("^.*\\.(webp)$");
	}

	static public boolean isPng(String string) {
		return string.toLowerCase().matches("^.*\\.(png)$");
	}

	static public boolean isSvg(String string) {
		return string.toLowerCase().matches("^.*\\.(svg)$");
	}

	static public boolean isGeneralImage(String string) {
		return string.toLowerCase().matches("^.*\\.(png|jpg|jpeg|gif|bmp|heic)$");
	}

	static public boolean isImage(String string) {
		return isGeneralImage(string) || string.toLowerCase().matches("^.*\\.(webp)$") || string.toLowerCase().matches("^.*\\.(svg)$");
	}
}
