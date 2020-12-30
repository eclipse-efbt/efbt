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
package org.eclipse.efbt.cocamo.smcubes.ui.sirius;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.efbt.cocamo.smcubes.query.core.CustomQuery;
import org.eclipse.efbt.cocamo.smcubes.model.core.DOMAIN;
import org.eclipse.efbt.cocamo.smcubes.model.core.MEMBER;

/**
 * The services class used by VSM.
 * 
 * @author Neil Mackenzie
 */
public class Services {
    
    /**
    * See http://help.eclipse.org/neon/index.jsp?topic=%2Forg.eclipse.sirius.doc%2Fdoc%2Findex.html&cp=24 for documentation on how to write service methods.
    */
    public EObject myService(EObject self, String arg) {
       // TODO Auto-generated code
      return self;
    }
    
    public EList<MEMBER> getMembers(DOMAIN self) {

    	return CustomQuery.getMembers( self);
     }
}
