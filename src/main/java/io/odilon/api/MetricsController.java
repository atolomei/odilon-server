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

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import io.odilon.log.Logger;
import io.odilon.model.MetricsValues;
import io.odilon.model.SystemInfo;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.service.ServerSettings;
import io.odilon.service.ObjectStorageService;
import io.odilon.traffic.TrafficPass;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * 
 * <p>
 * API endpoint for System Monitoring Metrics
 * </p>
 * 
 * <ul>
 * <li>/metrics</li>
 * <li>/metricscolloquial</li>
 * <li>/metricsinformal</li>
 * <li>/status</li>
 * <li>/systeminfo</li>
 * </ul>
 * 
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 * @see {@Link SystemMonitoringService}
 * 
 * 
 */
@RestController
public class MetricsController extends BaseApiController {

    @Autowired
    private final ServerSettings serverSettings;

    @Autowired
    public MetricsController(ObjectStorageService objectStorageService, VirtualFileSystemService virtualFileSystemService,
            SystemMonitorService monitoringService, ServerSettings settings) {

        super(objectStorageService, virtualFileSystemService, monitoringService);
        this.serverSettings = settings;
    }

    @RequestMapping(value = "/status", produces = "application/json", method = RequestMethod.GET)
    public Map<String, Object> getStatus() {
        TrafficPass pass = null;
        try {
            pass = getTrafficControlService().getPass();
            return serverSettings.toMap();
        } finally {
            getTrafficControlService().release(pass);
            mark();
        }
    }

    /**
     * @return info in JSON format
     */
    @RequestMapping(value = "/systeminfo", produces = "application/json", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<SystemInfo> getSystemInfo() {

        TrafficPass pass = null;

        try {
            pass = getTrafficControlService().getPass();
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(objectStorageService.getSystemInfo());

        } finally {
            getTrafficControlService().release(pass);
            mark();
        }
    }

    /**
     * <p>
     * in text format
     * </p>
     */
    @RequestMapping(value = "/metricscolloquial", produces = "text/plain", method = RequestMethod.GET)
    public ResponseEntity<String> getMetricsColloquial() {
        return getMetricsInformal();
    }

    /**
     * <p>
     * in text format
     * </p>
     */
    @RequestMapping(value = "/metricsinformal", produces = "text/plain", method = RequestMethod.GET)
    public ResponseEntity<String> getMetricsInformal() {

        TrafficPass pass = null;

        try {

            pass = getTrafficControlService().getPass();

            StringBuilder str = new StringBuilder();

            MetricsValues info = getSystemMonitorService().getMetricsValues();

            str.append("\n");
            str.append("\n");

            for (String s : this.serverSettings.getAppCharacterName())
                str.append("    " + s + "\n");

            Map<String, String> map = info.getColloquial();

            str.append("\n");
            str.append("\n");

            map.forEach((k, v) -> str.append("    " + k + " -> " + v + "\n\n"));

            str.append("\n");
            str.append("\n");

            return new ResponseEntity<String>(str.toString(), HttpStatus.OK);

        } finally {
            getTrafficControlService().release(pass);
            mark();
        }
    }

    /**
     * <p>
     * in JSON format
     * </p>
     */
    @RequestMapping(value = "/metrics", produces = "application/json", method = RequestMethod.GET)
    public ResponseEntity<MetricsValues> getMetrics() {

        TrafficPass pass = null;

        try {
            pass = getTrafficControlService().getPass();

            return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(getSystemMonitorService().getMetricsValues());

        } finally {
            getTrafficControlService().release(pass);
            mark();
        }
    }
}
