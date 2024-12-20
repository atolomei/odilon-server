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
package io.odilon.error;

import io.odilon.errors.OdilonErrorProxy;
import io.odilon.net.ErrorCode;
import io.odilon.net.ODHttpStatus;

/**
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class OdilonObjectNotFoundException extends OdilonServerAPIException {

    private static final long serialVersionUID = 1L;

    public OdilonObjectNotFoundException() {
        super();
    }

    public OdilonObjectNotFoundException(OdilonErrorProxy proxy) {
        super(proxy);
    }

    public OdilonObjectNotFoundException(ErrorCode errorCode, String message) {
        super(ODHttpStatus.NOT_FOUND, errorCode, message);
    }

    public OdilonObjectNotFoundException(String message) {
        super(ODHttpStatus.NOT_FOUND, ErrorCode.OBJECT_NOT_FOUND, message);
    }

}
