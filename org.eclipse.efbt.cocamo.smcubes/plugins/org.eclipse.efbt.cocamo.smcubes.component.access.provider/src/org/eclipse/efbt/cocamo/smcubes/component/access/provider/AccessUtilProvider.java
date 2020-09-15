package org.eclipse.efbt.cocamo.smcubes.component.access.provider;

import org.eclipse.efbt.cocamo.smcubes.component.access.api.AccessUtils;
import org.eclipse.efbt.cocamo.smcubes.component.access.jackcess.impl.JackcessUtil;


/**
 * Provider for AccessUtils, for now this always provides the Jackcess 
 * implementation of AccessUtils.
 *  
 * @author Neil Mackenzie
 *
 */
public class AccessUtilProvider {
	
	/**
	 * returns the AccessUtils implementation
	 */
	public static AccessUtils getAccessUtils()
	{
		return new JackcessUtil();
	}

}
