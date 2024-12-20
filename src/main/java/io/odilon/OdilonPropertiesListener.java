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

package io.odilon;

import java.util.Locale;
import java.util.Properties;

import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertiesPropertySource;

/**
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class OdilonPropertiesListener implements ApplicationListener<ApplicationEnvironmentPreparedEvent> {

    @Override
    public void onApplicationEvent(ApplicationEnvironmentPreparedEvent event) {
        ConfigurableEnvironment environment = event.getEnvironment();
        Properties props = new Properties();
        props.put("app.name", "Odilon");
        props.put("spring.servlet.multipart.max-file-size", "300GB");
        props.put("spring.servlet.multipart.max-request-size", "300GB");
        props.put("spring.main.banner-mode", "off");
        props.put("spring.main.log-startup-info", "false");
        props.put("server.error.whitelabel.enabled", "false");
        props.put("spring.main.lazy-initialization", "false");
        props.put("spring.output.ansi.enabled", "DETECT");

        environment.getPropertySources().addFirst(new PropertiesPropertySource("odilonProps", props));

        Locale.setDefault(Locale.ENGLISH);
    }
}

//props.put("spring.servlet.multipart.file-size-threshold", "12KB");
//props.put("spring.resources.cache.cachecontrol.max-age", "120");
//props.put("spring.resources.cache.cachecontrol.must-revalidate", "true");

// environment.getPropertySources().forEach( item ->
// logger.debug(item.toString()));
// logger.debug( environment.getProperty("server.ssl.enabled"));
// .forEach( item -> logger.debug(item.toString()));
