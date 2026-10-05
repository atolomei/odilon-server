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

import org.apache.wicket.markup.html.pages.RedirectPage;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.Model;

import io.wktui.nav.menu.LinkMenuItem;
import io.wktui.nav.menu.MenuItemPanel;
import io.wktui.nav.menu.NavDropDownMenu;
import wktui.base.ModelPanel;

/**
 * <p>
 * User area of the global top panel: drop-down menu with the authenticated
 * user name and a Sign out entry.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class UserGlobalTopPanel extends ModelPanel<String> {

	private static final long serialVersionUID = 1L;

	public UserGlobalTopPanel(String id, IModel<String> usernameModel) {
		super(id, usernameModel);
		setOutputMarkupId(true);
	}

	@Override
	public void onInitialize() {
		super.onInitialize();
		add(getMenu());
	}

	private NavDropDownMenu<Void> getMenu() {

		NavDropDownMenu<Void> menu = new NavDropDownMenu<Void>("userMenu");

		String username = (getModel() != null && getModel().getObject() != null && !getModel().getObject().isBlank()) ? getModel().getObject() : "[anonymous]";
		menu.setTitle(Model.of(username));
		menu.setSubtitle(Model.of(""));

		// Sign out
		menu.addItem(new io.wktui.nav.menu.MenuItemFactory<Void>() {
			private static final long serialVersionUID = 1L;

			@Override
			public MenuItemPanel<Void> getItem(String id) {
				return new LinkMenuItem<Void>(id) {
					private static final long serialVersionUID = 1L;

					@Override
					public void onClick() {
						setResponsePage(new RedirectPage("/logout"));
					}

					@Override
					public IModel<String> getLabel() {
						return getLabel("sign-out");
					}
				};
			}
		});

		return menu;
	}
}
