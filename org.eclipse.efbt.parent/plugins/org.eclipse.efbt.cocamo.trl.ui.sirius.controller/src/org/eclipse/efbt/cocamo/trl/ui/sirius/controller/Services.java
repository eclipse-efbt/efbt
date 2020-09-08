package org.eclipse.efbt.cocamo.trl.ui.sirius.controller;


import org.eclipse.efbt.cocamo.core.component.core.Controller;
import org.eclipse.efbt.controller.core.model.platform_call.PlatformCall;



/**
 * The services class used by VSM.
 */
public class Services {
    
    /**
    * See http://help.eclipse.org/neon/index.jsp?topic=%2Forg.eclipse.sirius.doc%2Fdoc%2Findex.html&cp=24 for documentation on how to write service methods.
    */
	 public void executePlatformCall(PlatformCall call) {

	    	
	    	Controller.executPlatfromCall(call);
	    }
}
