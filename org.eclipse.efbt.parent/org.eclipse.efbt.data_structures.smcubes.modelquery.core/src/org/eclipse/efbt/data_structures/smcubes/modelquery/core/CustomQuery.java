package org.eclipse.efbt.data_structures.smcubes.modelquery.core;

import java.util.Iterator;

import org.eclipse.efbt.data_structures.smcubes.model.core.DOMAIN;
import org.eclipse.efbt.data_structures.smcubes.model.core.MEMBER;
import org.eclipse.efbt.data_structures.smcubes.model.efbt_data_definition.MemberModule;
import org.eclipse.efbt.data_structures.smcubes.model.smcubes_model.SmcubesModel;
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

		SmcubesModel bm = Util.getDefaultBirdModel(domain);
		EList<MemberModule> birdMemberModules = bm.getMembers();
		
		for (Iterator iterator = birdMemberModules.iterator(); iterator.hasNext();) {
			MemberModule memberModule = (MemberModule) iterator.next();

			EList<MEMBER> birdMembers = memberModule.getMembers();
			for (Iterator iterator2 = birdMembers.iterator(); iterator2.hasNext();) {
				MEMBER member = (MEMBER) iterator2.next();

				if (member.getDomain_id().equals(domain))
					members.add(member);

			}

		}

		return members;

	}

}
