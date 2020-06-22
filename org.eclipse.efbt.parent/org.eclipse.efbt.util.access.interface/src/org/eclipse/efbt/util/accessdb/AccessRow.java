package org.eclipse.efbt.util.accessdb;

import java.util.Date;

public interface AccessRow {
	
	public String getString(String columnName);
	
	public int getInt(String columnName);

	public Date getDate(String columnName);

	public boolean getBoolean(String columnName);

}
