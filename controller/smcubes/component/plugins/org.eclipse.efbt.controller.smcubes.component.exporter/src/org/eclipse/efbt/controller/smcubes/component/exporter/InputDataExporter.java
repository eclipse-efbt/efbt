/*******************************************************************************
 * Copyright (c) 2020 Bird Software Solutions Ltd
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *    Neil Mackenzie - initial API and implementation
 *******************************************************************************/
package org.eclipse.efbt.controller.smcubes.component.exporter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.Collator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;



import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM;
import org.eclipse.efbt.cocalimo.smcubes.model.testing.Cell;
import org.eclipse.efbt.cocalimo.smcubes.model.testing.CellWithEnumeratedValue;
import org.eclipse.efbt.cocalimo.smcubes.model.testing.CellWithValue;
import org.eclipse.efbt.cocalimo.smcubes.model.testing.CubeData;
import org.eclipse.efbt.cocalimo.smcubes.model.testing.InputData;
import org.eclipse.efbt.cocalimo.smcubes.model.testing.RowData;
import org.eclipse.efbt.cocalimo.smcubes.model.testing.Test;
import org.eclipse.efbt.cocalimo.smcubes.model.testing.TestModule;
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
	public void exportInputData(TestModule testModule, String outputfileDirectory) {

		EList<Test> tests = testModule.getTests();

		// for each test, export the input data, the input daa may be in multiple tables
		for (Iterator iterator = tests.iterator(); iterator.hasNext();) {
			Test e2eTest = (Test) iterator.next();

			InputData inputData = (InputData) e2eTest.getInputData();
			EList<CubeData> tables = inputData.getSourceCubeData();

			// for each table find the header structure for the csv file, add this to the
			// file
			// and then add a row to the csv file for each row of input data
			for (Iterator iterator2 = tables.iterator(); iterator2.hasNext();) {
				CubeData CubeData = (CubeData) iterator2.next();

				CUBE columnStructuredEntity = CubeData.getCube();

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
				EList<RowData> rows = CubeData.getRows();
				for (Iterator iterator3 = rows.iterator(); iterator3.hasNext();) {
					RowData RowData = (RowData) iterator3.next();
					exportRowToCSV(theFile, RowData, orderedColumns, e2eTest.getName());

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
	 * @param RowData
	 * @param orderedColumns
	 * @param testName
	 */
	private void exportRowToCSV(FileWriter file, RowData RowData, List<String> orderedColumns,
			String testName) {
		try {
			String rowID = RowData.getRowID();
			file.append(testName + "," + rowID);
			EList<Cell> cells = RowData.getCells();

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
	private void appendColumnToFile(String columnname, EList<Cell> cells, 
			FileWriter file) throws IOException {

		// fine the cell in the list of cells which is associated with the 
		// column
		Cell actualCell = null;
		for (Iterator iterator = cells.iterator(); iterator.hasNext();) {
			Cell Cell = (Cell) iterator.next();
			if (Cell.getColumn().getName().equalsIgnoreCase(columnname)) {

				actualCell = Cell;
			}
		}
		// get the value of the cell, which might be an enumerated value or
		// just a value.
		if (actualCell != null) {
			file.append(',');
			if (actualCell instanceof CellWithEnumeratedValue)
				file.append(((CellWithEnumeratedValue) actualCell).
						getValue().getCode());
			else if (actualCell instanceof CellWithValue)
				file.append(((CellWithValue) actualCell).getValue());
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
