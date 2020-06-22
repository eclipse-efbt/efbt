package org.eclipse.efbt.util.access.impl.jackess; 

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


//import org.eclipse.efbt.util.accessdb.AccessRow;
//import org.eclipse.efbt.util.accessdb.AccessUtils;

import com.healthmarketscience.jackcess.*;

public class JackcessUtils  {

	
	public List<Row> getRowsForTable(String accessDB, String tableName) throws IOException {
		// TODO Auto-generated method stub
		List<Row> list = new ArrayList<Row>();
		{
			Table table;
			
				table = DatabaseBuilder.open(new File(accessDB)).getTable(tableName);
				for (Row row : table) {
					
					//AccessRow accessRow = new JackcessRow(row);
					list.add(row);
				}
			
		}
		
		return list;
					
				
	}

}
