package io.odilon.api;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import io.odilon.error.OdilonServerAPIException;
import io.odilon.log.Logger;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.net.ErrorCode;
import io.odilon.net.ODHttpStatus;
import io.odilon.service.ObjectStorageService;
import io.odilon.service.ServerSettings;
import io.odilon.traffic.TrafficControlService;
import io.odilon.traffic.TrafficPass;
import io.odilon.vfs.model.VirtualFileSystemService;


/**
 * <p>System operations</p>
 * 
 *  <ul>
 * 		<li>/wipeallpreviousversions</li>
 * 
 * 	</ul> 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class SystemController extends BaseApiController {


	@SuppressWarnings("unused")
	static private Logger logger = Logger.getLogger(SystemController.class.getName());
	
	@SuppressWarnings("unused")
	private ServerSettings settings;
	
	@Autowired
	public SystemController( 	ObjectStorageService objectStorageService, 
								VirtualFileSystemService virtualFileSystemService,
								SystemMonitorService monitoringService,
								ServerSettings settings, 
								TrafficControlService trafficControlService) {

		super(objectStorageService, virtualFileSystemService, monitoringService, trafficControlService);
		this.settings = settings;
	}
	
	/**
	 * <p>Wipe all previous versions for all Buckets. This command is Async, returns after adding a 
	 * ServiceRequest to the Scheduler</p>
	 */
	@RequestMapping(value = "/wipeallpreviousversions", method = RequestMethod.DELETE)
	public void wipeAllPreviousVersions() {
		
		TrafficPass pass = null;
		try {
			pass = getTrafficControlService().getPass();
			getObjectStorageService().wipeAllPreviousVersions();
		
		} catch (OdilonServerAPIException e) {
			throw e;
		} catch (Exception e) {
			throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
			mark();
		}
	}



}
