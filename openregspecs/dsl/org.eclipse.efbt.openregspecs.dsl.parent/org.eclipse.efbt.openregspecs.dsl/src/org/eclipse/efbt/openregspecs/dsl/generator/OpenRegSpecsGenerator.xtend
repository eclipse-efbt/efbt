/**Copyright (c) 2020 Bird Software Solutions Ltd
 All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html

SPDX-License-Identifier: EPL-2.0 

 Contributors:
 Neil Mackenzie - initial API and implementation*/
package org.eclipse.efbt.openregspecs.dsl.generator

import org.eclipse.emf.ecore.resource.Resource
import org.eclipse.xtext.generator.AbstractGenerator
import org.eclipse.xtext.generator.IFileSystemAccess2
import org.eclipse.xtext.generator.IGeneratorContext
import org.eclipse.efbt.openregspecs.model.open_reg_specs.XClass
import org.eclipse.efbt.openregspecs.model.open_reg_specs.XPackage
import org.eclipse.efbt.openregspecs.model.open_reg_specs.XAttribute
import org.eclipse.efbt.openregspecs.model.open_reg_specs.XReference
import org.eclipse.efbt.openregspecs.model.open_reg_specs.XEnum
import org.eclipse.efbt.openregspecs.model.open_reg_specs.XDataType
import org.eclipse.efbt.openregspecs.model.open_reg_specs.XOperation
import org.eclipse.efbt.openregspecs.model.open_reg_specs.UserTask
import org.eclipse.efbt.openregspecs.model.open_reg_specs.ServiceTask
import org.eclipse.efbt.openregspecs.model.open_reg_specs.Module
import org.eclipse.efbt.openregspecs.model.open_reg_specs.ViewModule
import org.eclipse.efbt.openregspecs.model.open_reg_specs.WorkflowModule
import java.io.File

/**
 * Generates code from your model files on save.
 * 
 * See https://www.eclipse.org/Xtext/documentation/303_runtime_concepts.html#code-generation
 */
class OpenRegSpecsGenerator extends AbstractGenerator {

	override void doGenerate(Resource resource, IFileSystemAccess2 fsa, IGeneratorContext context) {
		System.out.println("resource = " + resource)
		        for (xpackage : resource.allContents.toIterable.filter(XPackage)) {
                System.out.println("package =  " + xpackage.name)
        
		fsa.generateFile(xpackage.name + '.xcore',  '''

package «xpackage.name»
«IF xpackage.name.trim != "types"»
«FOR theImport : xpackage.imports»

«IF theImport.importedNamespace.trim != "types.*"»
import «theImport.importedNamespace» 
«ENDIF»
«ENDFOR»
«FOR xclass : xpackage.classifiers.filter(XClass)»
«IF xclass.abstract»abstract «ENDIF»class «xclass.name» «IF xclass.superTypes.length == 1» extends «xclass.superTypes.get(0).name» «ENDIF»{
«FOR xmember : xclass.members»  
«IF xmember instanceof XAttribute» 	«IF xmember.ID»id «ENDIF»«xmember.type.name» «IF xmember.upperBound == -1»[]  «ELSEIF !((xmember.lowerBound == 0) && (xmember.upperBound == 1)) »[«xmember.lowerBound»..«xmember.upperBound»]«ENDIF» «xmember.name» «ENDIF»
«IF xmember instanceof XReference» 	«IF xmember.containment»contains «ELSE»refers«ENDIF» «xmember.type.name» «IF xmember.upperBound == -1»[]  «ELSEIF !((xmember.lowerBound == 0) && (xmember.upperBound == 1)) »[«xmember.lowerBound»..«xmember.upperBound»]«ENDIF» «xmember.name»«ENDIF»	
«IF xmember instanceof XOperation» 	op «xmember.type.name» «IF xmember.upperBound == -1»[]  «ELSEIF !((xmember.lowerBound == 0) && (xmember.upperBound == 1)) »[«xmember.lowerBound»..«xmember.upperBound»]«ENDIF» «xmember.name»() 
	{
	 	«IF xmember.body !== null»«findXCoreSubstring(xmember.body)»«ENDIF»
	}
	«ENDIF»«ENDFOR» 
}
«ENDFOR»
«FOR xEnum : xpackage.classifiers.filter(XEnum)»
enum «xEnum.name» {«FOR xliteral : xEnum.literals»  «xliteral.name»  as "«xliteral.literal»"  = «xliteral.value» «ENDFOR»}
«ENDFOR»
«FOR xDataType : resource.allContents.filter(XDataType).toIterable»
«IF !(xDataType instanceof XEnum)»
type  «xDataType.name» wraps «IF xDataType.name == "Date"»java.util.Date «ELSE»«xDataType.name» «ENDIF» 
«ENDIF»	

«ENDFOR»
«ENDIF»
        ''')
         }

         	fsa.generateFile('rpmnutils' + File.separator +'RPMNUtils.java',  '''
 
package rpmnutils;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EParameter;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl;
import org.eclipse.emf.ecore.xmi.impl.XMIResourceFactoryImpl;

import F_01_01_REFoutput_logic.F_01_01_REFoutput_logicPackage;
import F_01_01_REFoutput_logic.impl.F_01_01_REFoutput_logicPackageImpl;
import output_layer_entities.Output_layer_entitiesFactory;
import output_layer_entities.Output_layer_entitiesPackage;

public class RPMNUtils {
	
	public static String fileDirectory = null;
	
	public static ResourceSet resSet =  null;
	
	
	public static void main(String[] args)
	{
		fileDirectory = args[0];
		EPackage.Registry.INSTANCE.put(F_01_01_REFoutput_logicPackage.eNS_URI, F_01_01_REFoutput_logicPackage.eINSTANCE);
		EPackage.Registry.INSTANCE.put(Output_layer_entitiesPackage.eNS_URI, Output_layer_entitiesPackage.eINSTANCE);
		
		Resource.Factory.Registry reg = Resource.Factory.Registry.INSTANCE;
		Map<String, Object> m = reg.getExtensionToFactoryMap();
		m.put("xmi", new XMIResourceFactoryImpl());
		resSet = new ResourceSetImpl();
		
		//look at the file names in the directory, find the one with _Expected_results at the end, 
		// this is the callss, the others provide the detials for input data.
		EList<EClassifier> outputClasses = Output_layer_entitiesPackage.eINSTANCE.getEClassifiers();
		EClass outputEClass = null;
		String className = getClassNameFromExpectedResultsFile();
		for (Iterator iterator = outputClasses.iterator(); iterator.hasNext();) {
			EClassifier eClassifier = (EClassifier) iterator.next();

			if (eClassifier.getName().equals(className))
				outputEClass = (EClass) eClassifier;
				
			
		}
		EObject table = Output_layer_entitiesFactory.eINSTANCE.create(outputEClass);
		Resource resource2 = resSet.createResource(URI.createFileURI(fileDirectory + File.separator +className + ".xmi"));

		resource2.getContents().add(table);
		EList<EOperation> operations = outputEClass.getEOperations();
		for (Iterator iterator2 = operations.iterator(); iterator2.hasNext();) {
			EOperation eOperation = (EOperation) iterator2.next();
			EList<EParameter> params = new BasicEList<EParameter>();
			if (eOperation.getName().equals("init"))
				try {
					table.eInvoke(eOperation, params);
				} catch (InvocationTargetException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
		}
		persistObject(table);
		
		TreeIterator<EObject> contents = table.eAllContents();
		
		while (contents.hasNext())
		{
			EObject o = contents.next();
			operations = o.eClass().getEAllOperations();
			System.out.println(o.eClass().getName() +" {");
			for (Iterator iterator3 = operations.iterator(); iterator3.hasNext();) {
				EOperation eOperation = (EOperation) iterator3.next();
				EList<EParameter> params = new BasicEList<EParameter>();
				if (!eOperation.getName().equals("init"))
					try {
						Object result = o.eInvoke(eOperation, params);
						System.out.println(result);
						
					} catch (InvocationTargetException e) {

						e.printStackTrace();
					}
					
			}
			System.out.println("}\n");

		}
			
		
		
	}
	private static String getClassNameFromExpectedResultsFile() {
		// TODO Auto-generated method stub
		//getTHe listOfFileNames
		File dir = new File(fileDirectory);
		File[] contents = dir.listFiles();
		String returnString = null;
		for (File object :contents)
		{
			String fileName = object.getName();
			if (fileName.endsWith("_ExpectedResults.csv"))
			{
				int index = fileName.indexOf("_ExpectedResults");
				returnString = fileName.substring(0, index);
			}
		}
		return returnString;
		
	}
	public static void init(EObject theObject)
	{
		if (resSet == null) 
			resSet = new ResourceSetImpl(); // theObject.eResource().getResourceSet();
		


		EClass eclass = theObject.eClass();
		EList<EReference> references = eclass.getEAllReferences();
		for (Iterator iterator = references.iterator(); iterator.hasNext();) {
			EReference eReference = (EReference) iterator.next();
			if (!eReference.isContainment())
			{
				EObject newObject = findTableInXMIFile(eReference.getEType(), eclass);
				if(newObject != null)
					theObject.eSet(eReference, newObject);
				else
				{
					newObject = createObjectFromReferenceType( eReference);
					
					EClass newObjectsClass = newObject.eClass();
					EList<EOperation> operations = newObjectsClass.getEOperations();
					for (Iterator iterator2 = operations.iterator(); iterator2.hasNext();) {
						EOperation eOperation = (EOperation) iterator2.next();
						EList<EParameter> params = new BasicEList<EParameter>();
						if (eOperation.getName().equals("init"))
							try {
								newObject.eInvoke(eOperation, params);
							} catch (InvocationTargetException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							
					}
					persistObject(newObject);
					System.out.println("theObject" + theObject);
					System.out.println("eReference" + eReference);
					System.out.println("eReference.etype" + eReference.getEType().getName());
					theObject.eSet(eReference, newObject);
				}
			}
			
			
		}

	}
	
	
	private static void persistObject(EObject theObject) {

	        try {
	        	theObject.eResource().save(Collections.EMPTY_MAP);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
	}


	private static EObject createObjectFromReferenceType( EReference eReference) {
		
		EClass theClass = (EClass) eReference.getEType();
		Resource resource1 = resSet.createResource(URI.createFileURI(fileDirectory + File.separator +theClass.getName() + ".xmi"));
		
		EObject newObject = theClass.getEPackage().getEFactoryInstance().create(theClass);
		resource1.getContents().add(newObject);
		return newObject;
		
	}


	private static EObject findTableInXMIFile(EClassifier eClassifier, EClass eclass) {

		EObject returnVal = null;
		EPackage ePackage = eclass.getEPackage(); 

		 Resource.Factory.Registry reg = Resource.Factory.Registry.INSTANCE;
		 Map<String, Object> m = reg.getExtensionToFactoryMap();
		 m.put("xmi", new XMIResourceFactoryImpl());

		
		try{
		Resource resource1  = resSet.getResource(URI.createFileURI(fileDirectory + File.separator + eClassifier.getName() +".xmi"), true);

		
		resource1.load(Collections.EMPTY_MAP);
		returnVal = resource1.getContents().get(0);
		}
		catch (Exception e) {
						// TODO Auto-generated catch block
						System.out.println(e);
					}
	
		return returnVal;
				
	}

}



'''
)
}
	
	def String findXCoreSubstring(String string) {
		var startIndex = string.indexOf("<xcore>")
		var endIndex = string.indexOf("</xcore>")
		var returnString = string
		if ( ( endIndex>0) && ( startIndex>-1))
			returnString = string.substring(startIndex+7,endIndex)
		return returnString
	}
	
}

 
	
 