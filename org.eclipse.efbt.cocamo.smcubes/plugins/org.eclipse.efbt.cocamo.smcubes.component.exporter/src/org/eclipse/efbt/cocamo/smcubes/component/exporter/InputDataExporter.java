package org.eclipse.efbt.cocamo.smcubes.component.exporter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.Collator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.eclipse.efbt.cocamo.core.model.test.E2ETest;
import org.eclipse.efbt.cocamo.core.model.test.TestModule;
import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.BaseCell;
import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.BaseCellWithEnumeratedValue;
import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.BaseCellWithValue;
import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.BaseColumnStructuredData;
import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.BaseRowData;
import org.eclipse.efbt.cocamo.smcubes.model.cocamo.Program;
import org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesTestInputData;
import org.eclipse.efbt.cocamo.smcubes.model.data_definition.CUBE;
import org.eclipse.efbt.cocamo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM;


import org.eclipse.emf.common.util.EList;



/**
 * InputDataExporter is responsible for exporting the input data of cocamo tests
 * into a CSV file format.
 * 
 * @author Neil Mackenzie
 *
 *
 */
public class InputDataExporter {

	public HashMap<CUBE, FileWriter> fileHashMap = new HashMap<CUBE, FileWriter>();

	/**
	 * Given an cocamo Program model instance, this method exports the input test
	 * data for all Tests contained in the Program.
	 * 
	 * The test are output to csv files in the outputfileDirectory provided.
	 * 
	 * @param program
	 * @param outputfileDirectory
	 */
	public void exportInputData(Program program, String outputfileDirectory) {

		TestModule testModule = program.getTests();
		EList<E2ETest> tests = testModule.getTests();

		// for each test, export the input data, the input daa may be in multiple tables
		for (Iterator iterator = tests.iterator(); iterator.hasNext();) {
			E2ETest e2eTest = (E2ETest) iterator.next();

			SMCubesTestInputData inputData = (SMCubesTestInputData) e2eTest.getInputData();
			EList<BaseColumnStructuredData> tables = inputData.getSmcubes_inputdata();

			// for each table find the header structure for the csv file, add this to the
			// file
			// and then add a row to the csv file for each row of input data
			for (Iterator iterator2 = tables.iterator(); iterator2.hasNext();) {
				BaseColumnStructuredData baseColumnStructuredData = (BaseColumnStructuredData) iterator2.next();

				CUBE columnStructuredEntity = baseColumnStructuredData.getCube();

				EList<CUBE_STRUCTURE_ITEM> columns = getCubseStructureItems( columnStructuredEntity);
				List<String> orderedColumns = getAlphabeticallyOrderedColumns(columns);

				FileWriter theFile = fileHashMap.get(columnStructuredEntity);
				if (theFile == null) {
					try {
						// add the header to the file
						File filefile = new File(outputfileDirectory + "/" + columnStructuredEntity.getName() + ".csv");
						theFile = new FileWriter(filefile);
						addHeaderToFile(theFile, orderedColumns);
						fileHashMap.put(columnStructuredEntity, theFile);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

				// add the rows of data to the file, ensuring the columns are in the correct
				// order
				EList<BaseRowData> rows = baseColumnStructuredData.getRows();
				for (Iterator iterator3 = rows.iterator(); iterator3.hasNext();) {
					BaseRowData baseRowData = (BaseRowData) iterator3.next();
					exportRowToCSV(theFile, baseRowData, orderedColumns, e2eTest.getName());

				}

			}

		}

	}

	private EList<CUBE_STRUCTURE_ITEM> getCubseStructureItems(CUBE columnStructuredEntity) {
		// TODO complete this
		return null;
	}

	/**
	 * add the correct header row with column names to a csv file
	 * 
	 * @param theFile
	 * @param orderedColumns
	 * @throws IOException
	 */
	private void addHeaderToFile(FileWriter theFile, List<String> orderedColumns) throws IOException {

		theFile.append("TestCaseID,RowdID,");
		for (Iterator iterator = orderedColumns.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();

			theFile.append(string);
			if (iterator.hasNext()) {
				theFile.append(',');
			}

		}
		theFile.append("\r\n");

	}

	/**
	 * Add a row of data to the file, ensuring the columns are in the correct order.
	 * 
	 * @param file
	 * @param baseRowData
	 * @param orderedColumns
	 * @param testName
	 */
	private void exportRowToCSV(FileWriter file, BaseRowData baseRowData, List<String> orderedColumns,
			String testName) {
		try {
			String rowID = baseRowData.getRowID();
			file.append(testName + "," + rowID);
			EList<BaseCell> cells = baseRowData.getCells();

			for (Iterator iterator2 = orderedColumns.iterator(); iterator2.hasNext();) {
				String columnname = (String) iterator2.next();
				appendColumnToFile(columnname, cells, file);
			}

			file.append("\r\n");
		} catch (IOException e) {

			e.printStackTrace();
		}

	}

	/**
	 * Given a column, find the value associated with that column and append it to a
	 * csv file.
	 * 
	 * @param columnname
	 * @param cells
	 * @param file
	 * @throws IOException
	 */
	private void appendColumnToFile(String columnname, EList<BaseCell> cells, 
			FileWriter file) throws IOException {

		// fine the cell in the list of cells which is associated with the 
		// column
		BaseCell actualCell = null;
		for (Iterator iterator = cells.iterator(); iterator.hasNext();) {
			BaseCell baseCell = (BaseCell) iterator.next();
			if (baseCell.getColumn().getName().equalsIgnoreCase(columnname)) {

				actualCell = baseCell;
			}
		}
		// get the value of the cell, which might be an enumerated value or
		// just a value.
		if (actualCell != null) {
			file.append(',');
			if (actualCell instanceof BaseCellWithEnumeratedValue)
				file.append(((BaseCellWithEnumeratedValue) actualCell).
						getValue().getCode());
			else if (actualCell instanceof BaseCellWithValue)
				file.append(((BaseCellWithValue) actualCell).getValue());
		} else {
			file.append(',');
		}

	}

	/**
	 * From a set of Columns, produce a list of column names in alphabetical 
	 * order
	 * @param columns
	 * @return
	 */
	private List<String> getAlphabeticallyOrderedColumns(EList<CUBE_STRUCTURE_ITEM> columns) {

		List<String> columnNames = new ArrayList<String>();

		for (Iterator iterator = columns.iterator(); iterator.hasNext();) {
			CUBE_STRUCTURE_ITEM column = (CUBE_STRUCTURE_ITEM) iterator.next();
			columnNames.add(column.getCube_variable_code());

		}
		java.util.Collections.sort(columnNames, Collator.getInstance());
		return columnNames;
	}

}
