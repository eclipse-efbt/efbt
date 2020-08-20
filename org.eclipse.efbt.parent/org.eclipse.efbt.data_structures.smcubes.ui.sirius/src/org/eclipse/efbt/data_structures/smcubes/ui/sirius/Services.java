package org.eclipse.efbt.data_structures.smcubes.ui.sirius;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.efbt.data_structures.smcubes.model.core.DOMAIN;
import org.eclipse.efbt.data_structures.smcubes.model.core.MEMBER;
import org.eclipse.efbt.data_structures.smcubes.modelquery.core.CustomQuery;

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
