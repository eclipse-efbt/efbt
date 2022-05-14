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
package org.eclipse.efbt.openregspecs.smcubes.query.core;

import java.util.Iterator;

import org.eclipse.efbt.openregspecs.model.module_management.Module;
import org.eclipse.efbt.openregspecs.model.module_management.ModuleDependency;
import org.eclipse.efbt.openregspecs.model.core.DOMAIN;
import org.eclipse.efbt.openregspecs.model.core.MEMBER;
import org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.DomainModule;
import org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.MemberModule;
import org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.SMCubesCoreModel;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;



/**
 * This class is responsable for storing any custom queries required in the
 * BIRD navigation tool.
 * 
 * @author Neil Mackenzie
 *
 */
public class CustomQuery {

	/**
	 * Given a domain, return a list of the members of the domain
	 * @param domain
	 * @return
	 */
	public static EList<MEMBER> getMembers(DOMAIN domain) {

		EList<MEMBER> members = new BasicEList<MEMBER>();

		EList<ModuleDependency> dependantModules = ((DomainModule) domain.eContainer()).getDependencies().getTheModules();
		
		
		
		for (Iterator iterator = dependantModules.iterator(); iterator.hasNext();) {
			ModuleDependency dependantModule =  (ModuleDependency) iterator.next();
			Module module = dependantModule.getTheModule();
					
			if( module instanceof MemberModule )
			{
				MemberModule memberModule = (MemberModule) module;
	
				EList<MEMBER> birdMembers = memberModule.getMembers();
				for (Iterator iterator2 = birdMembers.iterator(); iterator2.hasNext();) {
					MEMBER member = (MEMBER) iterator2.next();
	
					if (member.getDomain_id().getDomain_id().equals(domain.getDomain_id()))
						members.add(member);
	
				}
			}

		}

		return members;

	}

}
