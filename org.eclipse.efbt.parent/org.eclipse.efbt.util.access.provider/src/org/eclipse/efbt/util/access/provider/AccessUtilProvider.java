package org.eclipse.efbt.util.access.provider;

import org.eclipse.efbt.util.access.impl.jackcess.JackcessUtil;
import org.eclipse.efbt.util.accessdb.AccessUtils;

public class AccessUtilProvider {
	
	public static AccessUtils getAccessUtils()
	{
		return new JackcessUtil();
	}

}
