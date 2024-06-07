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
package io.odilon.api;

import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import io.odilon.error.OdilonServerAPIException;
import io.odilon.errors.OdilonErrorProxy;
import io.odilon.log.Logger;
import io.odilon.net.ErrorCode;

/**
 * <p>API Exception controller</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ControllerAdvice
public class OdilonExceptionAdvice {
			
	static private Logger logger = Logger.getLogger(OdilonExceptionAdvice.class.getName());
	
	@ExceptionHandler(OdilonServerAPIException.class)
	public ResponseEntity<OdilonErrorProxy> odilonExceptionHandler(OdilonServerAPIException ex) {
		
		ResponseEntity<OdilonErrorProxy> response = new ResponseEntity<OdilonErrorProxy>( 
				new OdilonErrorProxy(ex.getHttpsStatus(),
						ex.getErrorCode(),
						ex.getErrorMessage()),
				HttpStatus.valueOf(ex.getHttpsStatus())
		);
		return response;
	}
	
	
	@ExceptionHandler(Exception.class)
    public ResponseEntity<OdilonErrorProxy> handle(Exception ex) {
        
		logger.error("Server error -> " + ex.getClass().getName() + " | msg: " + ex.getMessage() + " | cause: " + ex.getCause());
		
		if (ex instanceof NullPointerException) {
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }
        
		OdilonErrorProxy p;
		
		if (ex instanceof org.springframework.web.multipart.MultipartException) {
			p = new OdilonErrorProxy(
					HttpStatus.INTERNAL_SERVER_ERROR.value(),
					ErrorCode.INTERNAL_MULTIPART_ERROR.value(),
					ex.getClass().getName() + " | msg: " + ex.getMessage() + " | cause: " + ex.getCause());
		}
		else {
		 p = new OdilonErrorProxy(
				HttpStatus.INTERNAL_SERVER_ERROR.value(),
				ErrorCode.INTERNAL_ERROR.value(),
				ex.getClass().getName() + " | " + ex.getMessage());
		}
		
		 ResponseEntity<OdilonErrorProxy> response = new ResponseEntity<OdilonErrorProxy>(p, HttpStatus.INTERNAL_SERVER_ERROR);
		 return response;
        
    }
	
}


