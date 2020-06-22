package org.eclipse.efbt.util.access.impl.jackcess;

import java.util.Date;

//import org.eclipse.efbt.util.accessdb.AccessRow;

import com.healthmarketscience.jackcess.Row;
import org.eclipse.efbt.util.accessdb.AccessRow;

public class JackcessRow  implements AccessRow{
	
	Row underlyingRow;

	public JackcessRow(Row row)
	{
		underlyingRow = row;
	}
	

	public String getString(String columnName) {
		// TODO Auto-generated method stub
		return underlyingRow.getString(columnName);
	}


	public int getInt(String columnName) {
		// TODO Auto-generated method stub
		return underlyingRow.getInt(columnName);
	}

	public Date getDate(String columnName) {
		// TODO Auto-generated method stub
		return underlyingRow.getDate(columnName);
	}


	public boolean getBoolean(String columnName) {
		// TODO Auto-generated method stub
		return false;
	}

}
