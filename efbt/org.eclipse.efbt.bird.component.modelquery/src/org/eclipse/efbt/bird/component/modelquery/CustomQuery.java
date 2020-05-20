package org.eclipse.efbt.bird.component.modelquery;

import java.util.Iterator;

import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;

import bird_model.BIRDModel;
import core.DOMAIN;
import core.MEMBER;
import efbt_data_definition.MemberModule;

public class CustomQuery {

	public static EList<MEMBER> getMembers(DOMAIN domain) {

		EList<MEMBER> members = new BasicEList<MEMBER>();

		BIRDModel bm = Util.getDefaultBirdModel(domain);
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
