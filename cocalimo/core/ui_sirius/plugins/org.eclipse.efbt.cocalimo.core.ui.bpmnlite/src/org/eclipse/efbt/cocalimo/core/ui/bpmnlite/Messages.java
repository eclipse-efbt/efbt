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
package org.eclipse.efbt.cocalimo.core.ui.bpmnlite;

import org.eclipse.osgi.util.NLS;

/**
 * 
 * @author Neil Mackenzie
 *
 */
public class Messages extends NLS {

	private static final String BUNDLE_NAME = "org.eclipse.efbt.cocalimo.core.ui.bpmnlite.messages"; //$NON-NLS-1$

	public static String PropertiesTitle;

	public static String CancelLabel;

	public static String AttributesLabel;

	public static String NameLabel;

	public static String NamingService_0;

	public static String HelpNameLabel;

	public static String TaskAttributeNaming;

	public static String DescriptionLabel;

	public static String HelpDescriptionLabel;

	static {
		// initialize resource bundle
		NLS.initializeMessages(BUNDLE_NAME, Messages.class);
	}

	private Messages() {
	}
}
