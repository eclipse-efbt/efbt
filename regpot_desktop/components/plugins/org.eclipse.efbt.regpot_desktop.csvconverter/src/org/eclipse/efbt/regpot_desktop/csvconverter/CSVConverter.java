/*******************************************************************************
 * Copyright (c) 2020 Bird Software Solutions Ltd
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDE-License-Identifier: EPL-2.0
 *
 * Contributors:
 *    Neil Mackenzie - initial API and implementation
 *******************************************************************************/
package org.eclipse.efbt.regpot_desktop.csvconverter;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.resource.ResourceSet;

public class CSVConverter {
public static String resourceURI = "";
	
	public static ResourceSet resSet =  null;
	
	public static boolean isSetUp = false;
	
	public static Path filePath = Path.of("logfile.txt");
	
	public static String directoryPath = null;
	
	public static void main(String args[])
	{
		Object inputPackage;
		try {
			
			Class inputPackageClass  = Class.forName("input_tables.Input_tablesPackage");

			Field[] fields = inputPackageClass.getDeclaredFields();
			
		 
		    Field field = inputPackageClass.getDeclaredField("eNS_URI");
		    Field field2 = inputPackageClass.getDeclaredField("eINSTANCE");
			field.setAccessible(true);
			field2.setAccessible(true);
			Object nsuri = field.get(inputPackageClass);
			Object iPackage = field2.get(inputPackageClass);

			EPackage.Registry.INSTANCE.put((String)nsuri,iPackage );

			} catch (ClassNotFoundException e) {

				e.printStackTrace();
			}
			 catch (SecurityException e) {

				e.printStackTrace();
			} catch (IllegalArgumentException e) {

				e.printStackTrace();
			}  catch (IllegalAccessException e) {

				e.printStackTrace();
			} catch (NoSuchFieldException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  
		
		directoryPath = args[0];
		EObjectToCSVConverter.resourceURI = directoryPath;
		//get the files in the directory
		String[] filepaths = getXMIFilesUnderDirectory(directoryPath);
		for (int i = 0; i < filepaths.length; i++) {
			if (filepaths[i].endsWith(".xmi"))
			{
				EObject theObject = EObjectLoader.loadObject(directoryPath + '/' + filepaths[i]);
				EObjectToCSVConverter.persistObjectAsCSV(theObject,false);
				EObjectToCSVConverter.persistObjectAsCSV(theObject,true);
			}
		}
	}

	private static String[] getXMIFilesUnderDirectory(String directoryPath) {
		String[] files = null;
		try { 
			  
            // Create a file object 
            File f = new File(directoryPath); 
  
            // Get all the names of the files present 
            // in the given directory 
            files = f.list(); 

        } 
        catch (Exception e) { 
            System.err.println(e.getMessage()); 
        } 
		return files;
    } 
	

}
