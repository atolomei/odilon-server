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
package io.odilon.web.search;

import java.util.Arrays;

import org.apache.wicket.ajax.AjaxRequestTarget;
import org.apache.wicket.ajax.form.AjaxFormComponentUpdatingBehavior;
import org.apache.wicket.ajax.markup.html.navigation.paging.AjaxPagingNavigator;
import org.apache.wicket.markup.html.form.ChoiceRenderer;
import org.apache.wicket.markup.html.form.DropDownChoice;
import org.apache.wicket.model.IModel;

import io.wktui.nav.menu.NavDropDownMenu;
import io.wktui.struct.list.ListPanelToolbar;

/**
 * <p>
 * Toolbar of the search {@link ResultsPanel}: sort selector on the left, page
 * navigator and total number of results on the right.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public abstract class SearchResultsToolbar extends ListPanelToolbar {

	private static final long serialVersionUID = 1L;

	public SearchResultsToolbar(String id, AjaxPagingNavigator navigator, NavDropDownMenu<Void> menu) {
		super(id, navigator, menu);
	}

	/** model of the selected {@link OrderOption} (lives in the ResultsPanel) */
	protected abstract IModel<OrderOption> getOrderModel();

	/** called when the user changes the sort option */
	protected abstract void onOrderChange(AjaxRequestTarget target);

	@Override
	public boolean isSearchButton() {
		return false;
	}

	@Override
	public void onClick(AjaxRequestTarget target) {
	}

	@Override
	public void onInitialize() {
		super.onInitialize();

		DropDownChoice<OrderOption> order = new DropDownChoice<OrderOption>("order", getOrderModel(), Arrays.asList(OrderOption.values()), new ChoiceRenderer<OrderOption>("label"));

		order.add(new AjaxFormComponentUpdatingBehavior("change") {
			private static final long serialVersionUID = 1L;

			@Override
			protected void onUpdate(AjaxRequestTarget target) {
				SearchResultsToolbar.this.onOrderChange(target);
			}
		});

		getContainer().add(order);
	}
}
