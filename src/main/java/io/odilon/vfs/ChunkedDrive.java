package io.odilon.vfs;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;


import io.odilon.log.Logger;

/**
* <p>For RAID 6</p>
* 
* @author atolomei@novamens.com (Alejandro Tolomei)
* 
* 
*/
@Component
@Scope("prototype")
public class ChunkedDrive extends ODDrive {

	@SuppressWarnings("unused")
	static private Logger logger = Logger.getLogger(ODSimpleDrive.class.getName());
	
	@Autowired
	protected ChunkedDrive(String rootDir) {
		super(rootDir);
	}
	
	protected ChunkedDrive(String name, String rootDir, int configOrder) {
		super(name, rootDir, configOrder);
	}
	
 
	


}