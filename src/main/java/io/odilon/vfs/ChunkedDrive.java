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
package io.odilon.vfs;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;


import io.odilon.log.Logger;

/**
* <p>Drive for RAID 6</p>
* 
* @author atolomei@novamens.com (Alejandro Tolomei)
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