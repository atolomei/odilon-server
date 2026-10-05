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
package io.odilon.web.panel;

import org.apache.wicket.markup.html.image.Image;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.Model;
import org.apache.wicket.request.resource.PackageResourceReference;

import io.wktui.nav.menu.MainMenu;
import wktui.base.ModelPanel;

/**
 * <p>
 * Global top panel included on all Odilon UI pages: brand and hamburger
 * {@link MainMenu} on the left (Home / Search / Info), {@link UserGlobalTopPanel}
 * on the right.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class GlobalTopPanel extends ModelPanel<String> {

	private static final long serialVersionUID = 1L;

	public GlobalTopPanel(String id, IModel<String> usernameModel) {
		super(id, usernameModel);
	}

	@Override
	public void onInitialize() {
		super.onInitialize();

		add(new Image("logo", new PackageResourceReference(GlobalTopPanel.class, "odilon-logo.png")));

		MainMenu mainMenu = new MainMenu("mainMenu");
		mainMenu.addLink(Model.of("Home"), "/ui");
		mainMenu.addLink(Model.of("Search"), "/ui/search");
		//mainMenu.addLink(Model.of("Info"), "/ui/info");
		add(mainMenu);

		add(new UserGlobalTopPanel("userGlobalTopPanel", getModel()));
	}
}
