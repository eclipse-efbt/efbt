package org.eclipse.efbt.controller.ui.sirius;


import org.eclipse.efbt.controller.component.Controller;
import org.eclipse.efbt.controller.model.platform_call.PlatformCall;


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
