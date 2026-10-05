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
package io.odilon.web;

import org.apache.wicket.Page;
import org.apache.wicket.spring.injection.annot.SpringComponentInjector;
import org.springframework.stereotype.Component;

import com.giffing.wicket.spring.boot.starter.app.WicketBootStandardWebApplication;

import io.odilon.web.home.HomePage;

/**
 * <p>
 * Wicket application for the Odilon Server user interface. Authentication is
 * handled by Spring Security (HTTP Basic / form login), so the standard
 * (non-secured) Wicket Boot application is used.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Component
public class OdilonWicketApplication extends WicketBootStandardWebApplication {

	@Override
	public Class<? extends Page> getHomePage() {
		return HomePage.class;
	}

	@Override
	public void init() {
		super.init();
		getFrameworkSettings().setSerializer(new org.apache.wicket.serialize.java.JavaSerializer(getApplicationKey()));
		getComponentInstantiationListeners().add(new SpringComponentInjector(this));
	}
}
