package org.eclipse.efbt.clan.ui.sirius.testmanager;

import java.util.Iterator;

import org.eclipse.efbt.functional_design.core.model.functionality_module.FunctionalityModule;
import org.eclipse.efbt.functional_design.core.model.functionality_module.ScenarioSetFunctionalityModule;
import org.eclipse.efbt.functional_design.core.model.functionality_module.SerialFunctionalityModule;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EObject;



/**
 * The services class used by VSM.
 * 
 * @author Neil Mackenzie
 */
public class Services {

	/**
	 * See
	 * http://help.eclipse.org/neon/index.jsp?topic=%2Forg.eclipse.sirius.doc%2Fdoc%2Findex.html&cp=24
	 * for documentation on how to write service methods.
	 */
	public EObject myService(EObject self, String arg) {

		return self;
	}

	/**
	 * Given an instance of a FunctionalityModule, this method returns the previous
	 * functinalmodule in a chain, or null if there was not a previous one.
	 * 
	 * @param self
	 * @return
	 */
	public EList<FunctionalityModule> getPreviousFunctionalModule(FunctionalityModule self) {

		EList<FunctionalityModule> list = new BasicEList<FunctionalityModule>();
		EObject container = self.eContainer();

		boolean addIt = false;
		if (container != null && ((container instanceof SerialFunctionalityModule)
				|| container instanceof ScenarioSetFunctionalityModule))
		// why do we include ScenarioSetFunctionalModule in the if statement,
		// there may be a good reason but should check.
		{
			EList<EObject> list2 = container.eContents();
			for (Iterator iterator = list2.iterator(); iterator.hasNext();) {
				EObject eObject = (EObject) iterator.next();

				if (addIt) {
					list.add((FunctionalityModule) eObject);
					addIt = false;
				}
				if (eObject == self)
					addIt = true;

			}
		}
		return list;
	}

}
