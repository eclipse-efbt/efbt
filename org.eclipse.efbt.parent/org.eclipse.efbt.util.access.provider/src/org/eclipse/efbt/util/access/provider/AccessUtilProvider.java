package org.eclipse.efbt.util.access.provider;

import org.eclipse.efbt.util.access.impl.jackcess.JackcessUtil;
import org.eclipse.efbt.util.accessdb.AccessUtils;


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
