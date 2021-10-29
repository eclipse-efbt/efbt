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
package org.eclipse.efbt.cocalimo.smcubes.ui.bpmnlite;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.sirius.diagram.DNode;
import org.eclipse.sirius.tools.api.ui.IExternalJavaAction;

/**
 * 
 * @author Neil Mackenzie
 *
 */
public class SetUnsetExternalLabels implements IExternalJavaAction {

	public SetUnsetExternalLabels() {
		// Nada
	}

	@Override
	public void execute(Collection<? extends EObject> selections, Map<String, Object> parameters) {
		if (parameters == null) {
			return;
		}

		Object o = parameters.get("views");
		if (o instanceof List) {
			List<Object> list = (List<Object>) o;
			for (Object oo : list) {
				if (oo instanceof DNode) {
					DNode dNode= (DNode)oo;
					if (ServiceHelper.isExternalLabel(dNode)) {
						ServiceHelper.setInternalLabel(dNode);
					} else {
						ServiceHelper.setExternalLabel(dNode);
					}
				}
			}
		}
	}

	@Override
	public boolean canExecute(Collection<? extends EObject> selections) {
		return true;
	}

}
