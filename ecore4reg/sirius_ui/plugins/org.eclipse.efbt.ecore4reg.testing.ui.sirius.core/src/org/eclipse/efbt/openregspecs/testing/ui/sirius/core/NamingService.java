/*******************************************************************************
 * Copyright (c) 2020 Bird Software Solutions Ltd
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *    Neil Mackenzie - initial API and implementation
 *******************************************************************************/
package org.eclipse.efbt.ecore4reg.testing.ui.sirius.core;


import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.edit.provider.ComposedAdapterFactory;
import org.eclipse.emf.edit.ui.provider.AdapterFactoryLabelProvider;
import org.eclipse.sirius.diagram.DNode;
import org.eclipse.efbt.ecore4reg.model.open_reg_specs.BaseElement;

import org.eclipse.efbt.ecore4reg.model.open_reg_specs.FlowElement;


import com.google.common.base.Strings;

/**
 * 
 * @author Neil Mackenzie
 *
 */
public class NamingService {

	/** Label provider */
	protected static final AdapterFactoryLabelProvider LABEL_PROVIDER = new AdapterFactoryLabelProvider(
			new ComposedAdapterFactory(ComposedAdapterFactory.Descriptor.Registry.INSTANCE));

	public String getPropertiesTitle(EObject eObj) {
		String msg = Messages.NamingService_0;
		msg = msg + LABEL_PROVIDER.getText(eObj);
		return msg;
	}

	public String getCancelLabel(EObject eObj) {
		return Messages.CancelLabel;
	}

	public String getAttributesLabel(EObject eObj) {
		return Messages.AttributesLabel;
	}

	public String getNameLabel(EObject eObj) {
		return Messages.NameLabel;
	}

	public String getHelpNameLabel(EObject eObj) {
		return Messages.HelpNameLabel;
	}

	public String getDescriptionLabel(EObject eObj) {
		return Messages.DescriptionLabel;
	}

	public String getHelpDescriptionLabel(EObject eObj) {
		return Messages.HelpDescriptionLabel;
	}

	public static String getNonExternalLabel(BaseElement fn, DNode dNode) {
		//if (ServiceHelper.isExternalLabel(dNode)) {
		//	return null;
		//}

		String name = getLabel(fn);

		return name == null ? "" : name;
	}

	public static String getLabel(BaseElement fn) {
		String name = "";
		if (fn instanceof FlowElement) {
			name = ((FlowElement) fn).getName();
		}

		return name == null ? "" : name;
	}

	public static String getExternalLabel(BaseElement fn) {
		String result = "";

		String name = getLabel(fn);
		if (!Strings.isNullOrEmpty(name)) {
			result = name;
		}

	

		return "".equals(result) ? null : result;
	}

	


	public int getExternalLabelSize(BaseElement be) {
		return 7;
	}

	

}
