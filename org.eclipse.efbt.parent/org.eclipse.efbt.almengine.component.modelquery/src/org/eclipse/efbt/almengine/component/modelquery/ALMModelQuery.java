package org.eclipse.efbt.almengine.component.modelquery;


import org.eclipse.efbt.almengine.model.cubes.cubes.BaseDeltaCube;
import org.eclipse.efbt.aorta.base_column_structured_data.BaseColumnStructuredData;
import org.eclipse.efbt.aorta.base_column_structured_data.BaseRowData;

public class ALMModelQuery {

public static String getBaseRowID(BaseRowData row) {

       	
    //	boolean isDelete = row.isIsDelete();
   // 	String rowToDelete = row.getRowIDToDelete();
    	String returnString="";
    	BaseColumnStructuredData baseColumnStructuredData = (BaseColumnStructuredData) row.eContainer();
    	
    	if(baseColumnStructuredData.getCube() instanceof BaseDeltaCube)
    	{
    	
	    	/**if (isDelete)
	    	{
	    		returnString = "delete row "+ rowToDelete;
	    	}
	    	else
	    	{
	    		if ((rowToDelete != null) && (rowToDelete.length() >0) )
	    		{    			
	    				returnString = "amend row " + rowToDelete;   		
	    		}
	    		else
	    		{
	    			returnString = "add row " + row.getRowID();
	    		}
	    	}*/
    		
    		returnString = "add row " + row.getRowID();
    	}
    	else
    	{
    		returnString = row.getRowID();
    	}
		
		return returnString;
   }
}
