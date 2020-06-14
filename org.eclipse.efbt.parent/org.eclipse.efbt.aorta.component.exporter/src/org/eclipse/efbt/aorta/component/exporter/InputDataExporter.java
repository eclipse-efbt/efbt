package org.eclipse.efbt.aorta.component.exporter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.Collator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.eclipse.emf.common.util.EList;

import org.eclipse.efbt.aorta.aorta_program.Program;
import org.eclipse.efbt.aorta.base_column_structured_data.BaseCell;
import org.eclipse.efbt.aorta.base_column_structured_data.BaseCellWithEnumeratedValue;
import org.eclipse.efbt.aorta.base_column_structured_data.BaseCellWithValue;
import org.eclipse.efbt.aorta.base_column_structured_data.BaseColumnStructuredData;
import org.eclipse.efbt.aorta.base_column_structured_data.BaseRowData;
import org.eclipse.efbt.aorta.column_structures.Column;
import org.eclipse.efbt.aorta.column_structures.ColumnStructuredEntity;
import org.eclipse.efbt.aorta.test.E2ETest;

import org.eclipse.efbt.aorta.test.TestModule;
import org.eclipse.efbt.aorta.test_input_data.TestColumnStructuredData;
import org.eclipse.efbt.aorta.test_input_data.TestInputData;

public class InputDataExporter {
	
	public HashMap<ColumnStructuredEntity, FileWriter> fileHashMap = new HashMap<ColumnStructuredEntity, FileWriter>();
	
	public void exportInputData(Program program, String outputfileDirectory) 
	{
		
		
		TestModule testModule = program.getTests();
		EList<E2ETest> tests = testModule.getTests();
		
		for (Iterator iterator = tests.iterator(); iterator.hasNext();) {
			E2ETest e2eTest = (E2ETest) iterator.next();
			
			TestColumnStructuredData inputData = (TestColumnStructuredData) e2eTest.getInputData();
			EList<BaseColumnStructuredData> tables = inputData.getSourceTableData();
			
			
			for (Iterator iterator2 = tables.iterator(); iterator2.hasNext();) {
				BaseColumnStructuredData baseColumnStructuredData = (BaseColumnStructuredData) iterator2.next();
			
				ColumnStructuredEntity columnStructuredEntity = baseColumnStructuredData.getCube();
			
				EList<Column> columns = columnStructuredEntity.getColumn();
				List<String> orderedColumns = getAlphabeticallyOrderedColumns(columns);
				
				FileWriter theFile = fileHashMap.get(columnStructuredEntity);
				if ( theFile  == null)
				{
					try {
						File filefile = new File(outputfileDirectory + "/" + columnStructuredEntity.getName() + ".csv");
						theFile = new FileWriter(filefile);
						addHeaderToFile(theFile,orderedColumns);
						fileHashMap.put(columnStructuredEntity, theFile);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				
				EList<BaseRowData> rows = baseColumnStructuredData.getRows();
				for (Iterator iterator3 = rows.iterator(); iterator3.hasNext();) {
					BaseRowData baseRowData = (BaseRowData) iterator3.next();
					exportRowToCSV(theFile, baseRowData, orderedColumns, e2eTest.getName());
					
				}
				
			}
			
		}
		//close each filewriter
		
	}

	private void addHeaderToFile(FileWriter theFile, List<String> orderedColumns) throws IOException {
		
		theFile.append("TestCaseID,RowdID,");
		for (Iterator iterator = orderedColumns.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();
			
			theFile.append(string);
			if (iterator.hasNext())
			{
				theFile.append(',');
			}

		}
		theFile.append("\r\n");
		
	}

	private void exportRowToCSV(FileWriter file, BaseRowData baseRowData, List<String> orderedColumns, String testName) {
		try {
		String rowID = baseRowData.getRowID();
		file.append(testName + "," + rowID );
		EList<BaseCell> cells = baseRowData.getCells();
		
		

				
		for (Iterator iterator2 = orderedColumns.iterator(); iterator2.hasNext();) {
			String columnname = (String) iterator2.next();
			appendColumnToFile(columnname,cells,file) ;
		}
		
				
				
				file.append("\r\n");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			
			
			
		}
		


	private void appendColumnToFile(String columnname, EList<BaseCell> cells, FileWriter file) throws IOException {
		
		BaseCell actualCell = null;
		for (Iterator iterator = cells.iterator(); iterator.hasNext();) {
			BaseCell baseCell = (BaseCell) iterator.next();
			if(baseCell.getColumn().getName().equalsIgnoreCase(columnname))
			{
				
				actualCell = baseCell;
			}
		}
			
			if(actualCell != null)
			{
				file.append(',');
				if(actualCell instanceof BaseCellWithEnumeratedValue)
					file.append(((BaseCellWithEnumeratedValue) actualCell).getValue().getCode());
				else if (actualCell instanceof BaseCellWithValue)
					file.append(((BaseCellWithValue) actualCell).getValue());
			}
			else
			{
				file.append(',');
			}
				
				
				
		}
		
	

	private List<String> getAlphabeticallyOrderedColumns(EList<Column> columns) {
		// TODO Auto-generated method stub
		
		List<String> columnNames = new ArrayList<String>();
		
		for (Iterator iterator = columns.iterator(); iterator.hasNext();) {
			Column column = (Column) iterator.next();
			columnNames.add(column.getName());
			
		}
		java.util.Collections.sort(columnNames, Collator.getInstance());
		return columnNames;
	}

}
