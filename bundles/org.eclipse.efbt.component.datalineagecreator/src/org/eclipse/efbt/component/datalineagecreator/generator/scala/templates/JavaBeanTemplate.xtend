/*******************************************************************************
 * Copyright (c) 2020 Bird Software Solutions Ltd
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v20.html
 *
 * Contributors:
 *     Neil Mackenzie - initial API and implementation
 *******************************************************************************/
 package org.eclipse.efbt.component.datalineagecreator.generator.scala.templates


import org.eclipse.emf.ecore.resource.Resource
import org.eclipse.xtext.generator.IFileSystemAccess
import org.eclipse.xtext.generator.IGenerator
import org.eclipse.emf.common.util.EList
import cube_transformation_logic.CubeTransformationLogic
import row_transformation_logic.BaseRowStructure
import base_cube_data.BaseCubeData
import column_transformation_logic.ColumnFunction
import org.eclipse.efbt.component.common.util.ScalaRuntimeUtil
import column_transformation_logic.StandardBasicColumnFunction
import base_cube_data.BaseCell
import row_transformation_logic.RowJoinFunction
import row_transformation_logic.OneToOneRowCreationApproach
import java.util.List
import java.util.ArrayList
import functions.BooleanFunction
import row_transformation_logic.FilterRowCreationApproach
import functions.BasicFunction
import row_transformation_logic.GroupByRowCreationApproach
import column_transformation_logic.AggregateColumnFunction
import row_transformation_logic.FilterAndGroupToOneRowCreationApproach
import functions.ResolvedCubeColumnParameter
import functions.AggregateFunction
import trl_sql_views.SQLView
import core.VARIABLE
import base_cube_data.BaseCellWithEnumeratedValue
import base_cube_data.BaseCellWithValue
import core.FACET_VALUE_TYPE
import cubes.FreeBirdToolsCube
import platform_call.ExecuteAttributeLineageModel
import attribute_lineage.AttributeLineageModel
import row_transformation_logic.UnionRowCreationApproach
import org.eclipse.efbt.component.common.util.AttributeLineageUtil
import cubes.DeltaAccumulationCube
import row_transformation_logic.BaseViewIncorporatingDeltasRowFunction
import cubes.BaseViewCube
import cubes.BaseDeltaCube
import row_transformation_logic.DeltaAccumulationRowFunction

/**
 * Generateor tmplate for generating scala files
 */
class JavaBeanTemplate implements IGenerator {

	/**
	 * Generate
	 */
	override doGenerate(Resource input, IFileSystemAccess fsa) {

		
		input.allContents.filter(ExecuteAttributeLineageModel).forEach[
		
			
		    val almName = it.name
			val content2 = generateTheCellModelCreator(it)
		   fsa.generateFile(almName +"_TheCellModelCreator.scala", content2)
			val sourceCubes = it.programInputs.sourceTableData
			sourceCubes.forEach[
				
				val content = generateScalaCodeForTableData(it)
				val cubename = it.cube.cube_name
				
				fsa.generateFile(almName  + "_"+ cubename + "_Data.scala", content)
				
			]
			val rowLogicGroup = it.attributeLineageModel
			rowLogicGroup.rowTransformations.forEach[
			
			
					val cubename = it.rowCreationApproachForCube.cube.cube_name
					
					val content = generateScalaCodeForRowLogic(it,it.rowCreationApproachForCube.cube.cube_name,it.getTheDependantCubeeNames, it.getTheDependantCubes)
					
				
					
				fsa.generateFile(almName  +  "_"+ cubename + ".scala", content)
			

			]
		
		rowLogicGroup.baseSchemas.forEach[
			
				
					val content = generateSourceSchemaScalaCode(it)
					val cubename = it.cube.cube_name
					System.out.println("SourceSchema" + cubename)
					fsa.generateFile(almName  +  "_"+ cubename +".scala", content)
			
				
			]
			
		]
	
		 
	}
	

	/**
	 * Get the user friendly name of an SQL view
	 */
	def String getToUserFriendlyString(SQLView view)
	{
		return view.eClass.name
	}
	
	/**
	 * Generate the TheCellModelCreator scala class. This class calls all the other Scala classes. 
	 */
	def generateTheCellModelCreator(ExecuteAttributeLineageModel test)  '''
		
		«val outputdir = test.outputdir.addbackslashes»
		«val almName = test.attributeLineageModel.name»
		«FOR schema:test.attributeLineageModel.baseSchemas»
				import «schema.cube.cube_name»_def._  
				import «schema.cube.cube_name»_data._  
		«ENDFOR»
		«FOR logic:test.attributeLineageModel.rowTransformations»
		import «logic.rowCreationApproachForCube.cube.cube_name»_def._  
		«ENDFOR»
		
		object TheCellModelCreator extends App {
		  override def  main(args: Array[String]) {
		«FOR schema:test.attributeLineageModel.baseSchemas»
		 val t_«schema.cube.cube_name»: RTable«schema.cube.cube_name»  = RTable«schema.cube.cube_name»(«schema.cube.cube_name»Data.initialiseRowList()) 		 
		«ENDFOR»
		«val orderedRowTransformation = test.attributeLineageModel.orderedRowTransformations»
		«FOR logic:orderedRowTransformation»
		 val t_«logic.rowCreationApproachForCube.cube.cube_name»: RTable«logic.rowCreationApproachForCube.cube.cube_name»= RTable«logic.rowCreationApproachForCube.cube.cube_name»(«FOR sourceTableName:logic.getTheDependantCubeeNames SEPARATOR ','»t_«sourceTableName»«ENDFOR»)				
		«ENDFOR»
		
		def stringForHeader = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + "<data_lineage:DataLineageModel xmlns:data_lineage=\"http://www.eclipse.org/data_lineage\" xmlns:base_cube_data=\"http://www.eclipse.org/base_cube_data\" xmlns:functions=\"http://www.eclipse.org/functions\" >\n" 
						  
				val stringForFile = stringForHeader + 
				«FOR logic:test.attributeLineageModel.rowTransformations SEPARATOR "+"»stringFor«logic.rowCreationApproachForCube.cube.cube_name»Table(t_«logic.rowCreationApproachForCube.cube.cube_name») «ENDFOR»+ «FOR schema:test.attributeLineageModel.baseSchemas SEPARATOR "+"»stringFor«schema.cube.cube_name»Table(t_«schema.cube.cube_name») «ENDFOR» +stringForEndOfXML()
				
					import java.io._
				    val pw = new PrintWriter(new File("«outputdir»\\«almName»_generated.data_lineage" ))
				    pw.write(stringForFile)
				    pw.close
		}
		«FOR sourceTableSchema:test.programInputs.sourceTableData»
		
			«val theSchemaName = sourceTableSchema.cube.cube_name»
							def  stringFor«theSchemaName»Table(table_«theSchemaName»: RTable«theSchemaName» ) :String = 
									{				 
									   "<sourceData name=\"«theSchemaName»\" cube=\"..\\extra\\«sourceTableSchema.cube.eResource.URI.lastSegment»#«sourceTableSchema.cube.cube_name»\"> \n" +
									   «FOR row:sourceTableSchema.rows»
									    "<rows rowID=\"«row.rowID»\" isDelete=\"«row.isIsDelete»\" rowIDToDelete=\"«row.rowIDToDelete»\">\n" +
									   	«FOR cell: row.cells»
									   	«val column= cell.column»
									   	«IF column.usedInSubsetsSchema(test,theSchemaName)»"<cells xsi:type=\"base_cube_data:«IF cell instanceof BaseCellWithEnumeratedValue»BaseCellWithEnumeratedValue«ENDIF»«IF cell instanceof BaseCellWithValue»BaseCellWithValue«ENDIF»\" cellID=\"«cell.cellID»\" column=\"..\\bird_import\\«column.eResource.URI.lastSegment»#«column.variable_id»\" value=\"«IF cell instanceof BaseCellWithEnumeratedValue»..\\bird_import\\«cell.value.eResource.URI.lastSegment»#«cell.value.member_id»«ENDIF»«IF cell instanceof BaseCellWithValue»«cell.value»«ENDIF»\"/>\n" +					   						     					   						     
									   		«ENDIF»								   
									   	 «ENDFOR»
									   "</rows>\n" +
									   «ENDFOR»
									   "</sourceData>\n "
								     
									}
	«ENDFOR»
						
							
					        			
	
		«FOR logic:test.attributeLineageModel.rowTransformations»
		
		
		«val logicName = logic.rowCreationApproachForCube.cube.cube_name»
		
		«val sourceTableNames =logic.getTheDependantCubeeNames»
		def  stringFor«logicName»Table(table_«logicName»: RTable«logicName» ) :String = 
		{
		   
		  
		   "<dataTraceableByCell name=\"«logicName»\" rowCreationApproachForCube=\"..\\platform_calls\\«logic.rowCreationApproachForCube.eResource.URI.lastSegment»#«logic.rowCreationApproachForCube.name»\" sourceCubeData=\"«FOR stn:sourceTableNames SEPARATOR ' '»#«stn»«ENDFOR»\" cube=\"..\\extra\\«logic.rowCreationApproachForCube.cube.eResource.URI.lastSegment»#«logic.rowCreationApproachForCube.cube.cube_name»\"> \n" +
		   «IF !(logic.rowCreationApproachForCube.cube instanceof DeltaAccumulationCube)»stringFor«logicName»Table2(«logicName»Def.getRRows«logicName»(table_«logicName»)) +   «ENDIF»
		   "</dataTraceableByCell>\n "
		     
		}
		
		«IF !(logic.rowCreationApproachForCube.cube instanceof DeltaAccumulationCube)»
		def  stringFor«logicName»Table2(rows :  List[RRow«logicName»] ) :String    = rows match
		{
		    
		   case tail :+  head  => 
		     stringFor«logicName»Row(head) + 
		     stringFor«logicName»Table2(tail) 
		   case List() => ""  
		} 
		
		
		def stringFor«logicName»Row(row : RRow«logicName») :String = 
		{
		   val rowID = «logicName»Def.getconciseid(row)
		  
		   «IF (!(logic.rowCreationApproachForCube.rowCreationApproach instanceof GroupByRowCreationApproach) && !(logic.rowCreationApproachForCube.rowCreationApproach instanceof FilterAndGroupToOneRowCreationApproach) && !(logic.rowCreationApproachForCube.rowCreationApproach instanceof BaseViewIncorporatingDeltasRowFunction))»
		   «FOR stn:sourceTableNames»
		   		  			val sourcerow«stn» = row.«stn»
		   		  			val sourcerowID«stn» =«stn»Def.getconciseid(sourcerow«stn»)
		   		  «ENDFOR»	
		   		  		    "<rows rowID=\"" +rowID + "\" sourceRows=\"" +  «FOR stn:sourceTableNames SEPARATOR "+\" \" +"»"#" + sourcerowID«stn»«ENDFOR»+"\">\n" +
		   		  		stringForRow«logicName»(row)  + 
		   		  		"</rows>\n" 
		   
		  «ELSEIF  (logic.rowCreationApproachForCube.rowCreationApproach instanceof BaseViewIncorporatingDeltasRowFunction)» 
		  
		   «val stn = (logic.rowCreationApproachForCube.rowCreationApproach as  BaseViewIncorporatingDeltasRowFunction).ultimateCubeSource.cube_name»
		  		   		  			val sourcerow«stn» = row.«stn»
		  		   		  			val sourcerowID«stn» =«stn»Def.getconciseid(sourcerow«stn»)
		  		   		  
		  		   		  		    "<rows rowID=\"" +rowID + "\" sourceRows=\"" +  "#" + sourcerowID«stn»+"\">\n" +
		  		   		  		stringForRow«logicName»(row)  + 
		  		   		  		"</rows>\n" 
		  

		 «ELSE»
		 
				    "<rows rowID=\"" +rowID + "\" sourceRows=\"" + getListOfIdsforSourceRowsOf«logicName»(row) + "\">\n" +		  		  
		  		stringForGroupRow«logicName»(row)  + 
		  		"</rows>\n" 
		  
		  «ENDIF»
		  
		}
«ENDIF»
		 «IF ((logic.rowCreationApproachForCube.rowCreationApproach instanceof GroupByRowCreationApproach) || (logic.rowCreationApproachForCube.rowCreationApproach instanceof FilterAndGroupToOneRowCreationApproach))»
				  
		def  getListOfIdsforSourceRowsOf«logicName»(row : RRow«logicName» ) :String =
		{
			 getListOfIdsforSourceRowsOf«logicName»2(row.«sourceTableNames.get(0)»)
			
		}
		def  getListOfIdsforSourceRowsOf«logicName»2(rows: List[RRow«sourceTableNames.get(0)»] ) :String =  rows match
				{
					  case  List() :+ head=>  "#" +«sourceTableNames.get(0)»Def.getconciseid(head)
					  case  tail :+ head =>   "#" +«sourceTableNames.get(0)»Def.getconciseid(head) +" " + getListOfIdsforSourceRowsOf«logicName»2(tail) 
					 		       case List() => "" 
					
				}
		«ENDIF»
		 «IF ((logic.rowCreationApproachForCube.rowCreationApproach instanceof GroupByRowCreationApproach) || (logic.rowCreationApproachForCube.rowCreationApproach instanceof FilterAndGroupToOneRowCreationApproach))»
		def getParamsForTheSourceRowsOf«logicName»(«logicName»Row :RRow«logicName», col :String) :String =  {
		
		   getParamsForSourceRowsOf«logicName»(«logicName»Row.«sourceTableNames.get(0)», col)
		}
		 «ENDIF»
		 		 
		 		 «IF !(logic.rowCreationApproachForCube.cube instanceof DeltaAccumulationCube)»  
		 		«IF !(logic.rowCreationApproachForCube.cube instanceof BaseViewCube)»
		def getParamsForSourceRowsOf«logicName»(«sourceTableNames.get(0)»Rows :List[RRow«sourceTableNames.get(0)»], col :String) :String = «sourceTableNames.get(0)»Rows match {
		  case  tail :+ head => getParamsForSourceRowsOf«logicName»(tail,col) +   getParamForSourceRowOf«logicName»(head,col) 
		       case List() => "" 
		}
		
		def getParamForSourceRowOf«logicName»(«sourceTableNames.get(0)»Row :RRow«sourceTableNames.get(0)», col :String) :String = {
		  
		  val rowId = «sourceTableNames.get(0)»Def.getconciseid(«sourceTableNames.get(0)»Row)
		  "     <parameters xsi:type=\"functions:CellsParameter\" cells=\"#" + rowId +":" +col+ "\"/>\n"
		}
		«ENDIF»
		«IF (logic.rowCreationApproachForCube.cube instanceof BaseViewCube)» 
			«val stn = logic.rowCreationApproachForCube.ultimateCubeSource.cube_name»
							def getParamsForSourceRowsOf«logicName»(«stn»Rows :List[RRow«stn»], col :String) :String = «stn»Rows match {
							  case  tail :+ head => getParamsForSourceRowsOf«logicName»(tail,col) +   getParamForSourceRowOf«logicName»(head,col) 
							       case List() => "" 
							}
							
							def getParamForSourceRowOf«logicName»(«stn»Row :RRow«stn», col :String) :String = {
							  
							  val rowId = «stn»Def.getconciseid(«stn»Row)
							  "     <parameters xsi:type=\"functions:CellsParameter\" cells=\"#" + rowId +":" +col+ "\"/>\n"
							}
		«ENDIF»	 
		
		
		def stringForGroupRow«logicName»(row : RRow«logicName») :String     =  { 
					
							 
					val rowID = «logicName»Def.getconciseid(row)
					 
					 
				  «FOR column:logic.columnFunctionGroup.columnFunctions»
				  
				  val «column.variable.variable_id.toLowerCase» = «logicName»Def.get«column.variable.variable_id.toLowerCase»(row)
				  
				
				 val «column.variable.variable_id.toLowerCase»Text =	«IF column.usedInSubsets == column.usedInSubsets»"<formulaCells cellID=\"" + rowID + ":«column.variable.variable_id»\" column=\"..\\bird_import\\«column.variable.eResource.URI.lastSegment»#«column.variable.variable_id»\" value=\"" + «column.variable.variable_id.toLowerCase» + "\">\n" +
				    «IF column instanceof AggregateColumnFunction»
				    «val aggregateFunction = column.aggregateFunction»
				    «aggregateFunction.getXMLForAggregateFunctionParameterisedBySourceRowID(logicName)»
				     «ENDIF»
	      "   </formulaCells>\n"
	      «ENDIF» ««««IF !column.usedInSubsets»""«ENDIF»
				  «ENDFOR»
				
				   «FOR column:logic.columnFunctionGroup.columnFunctions SEPARATOR "+"»«column.variable.variable_id.toLowerCase»Text«ENDFOR»
			   
				} 
		
		«IF !(logic.rowCreationApproachForCube.cube instanceof BaseViewCube)» 
		def stringForRow«logicName»(row : RRow«logicName») :String     =  { 
			
					 
			val rowID = «logicName»Def.getconciseid(row)
			 
			 «FOR stn:sourceTableNames»
			 «IF (!(logic.rowCreationApproachForCube.rowCreationApproach instanceof GroupByRowCreationApproach) && !(logic.rowCreationApproachForCube.rowCreationApproach instanceof FilterAndGroupToOneRowCreationApproach))»
			val sourcerow«stn» = row.«stn»
			val sourcerowID«stn» =«stn»Def.getconciseid(sourcerow«stn»)
			«ENDIF»
			  «ENDFOR»
		  «FOR column:logic.columnFunctionGroup.columnFunctions»
		  val «column.variable.variable_id.toLowerCase» = «logicName»Def.get«column.variable.variable_id.toLowerCase»(row)
		 
		
		 val «column.variable.variable_id.toLowerCase»Text =	«IF column.usedInSubsets == column.usedInSubsets»"<formulaCells cellID=\"" + rowID + ":«column.variable.variable_id»\" column=\"..\\bird_import\\«column.variable.eResource.URI.lastSegment»#«column.variable.variable_id»\" value=\"" + «column.variable.variable_id.toLowerCase» + "\">\n" +
		    «IF (!(logic.rowCreationApproachForCube.rowCreationApproach instanceof GroupByRowCreationApproach) && !(logic.rowCreationApproachForCube.rowCreationApproach instanceof FilterAndGroupToOneRowCreationApproach))»
		    «IF column instanceof StandardBasicColumnFunction»
		    «val basicfunction = column.basicFunction»
		    «basicfunction.getXMLForBasicFunctionParameterisedBySourceRowID()»
		     «ENDIF»
		     «ENDIF»
		     "   </formulaCells>\n"
		     «ENDIF»««««IF !column.usedInSubsets»""«ENDIF»
		  «ENDFOR»
		
		   «FOR column:logic.columnFunctionGroup.columnFunctions SEPARATOR "+"»«column.variable.variable_id.toLowerCase»Text«ENDFOR»
	   
		} 
		«ENDIF»
		
		«IF (logic.rowCreationApproachForCube.cube instanceof BaseViewCube)» 
				«val stn = logic.rowCreationApproachForCube.ultimateCubeSource .cube_name»
						def stringForRow«logicName»(row : RRow«logicName») :String     =  { 
							
									 
							val rowID = «logicName»Def.getconciseid(row)
							 
							
							 «IF (!(logic.rowCreationApproachForCube.rowCreationApproach instanceof GroupByRowCreationApproach) && !(logic.rowCreationApproachForCube.rowCreationApproach instanceof FilterAndGroupToOneRowCreationApproach))»
							val sourcerow«stn» = row.«stn»
							val sourcerowID«stn» =«stn»Def.getconciseid(sourcerow«stn»)
							«ENDIF»
							 
						  «FOR column:logic.columnFunctionGroup.columnFunctions»
						  val «column.variable.variable_id.toLowerCase» = «logicName»Def.get«column.variable.variable_id.toLowerCase»(row)
						 
						 
						 val «column.variable.variable_id.toLowerCase»Text =	«IF column.usedInSubsets == column.usedInSubsets»"<formulaCells cellID=\"" + rowID + ":«column.variable.variable_id»\" column=\"«column.variable.eResource.URI.toString»#«column.variable.variable_id»\" value=\"" + «column.variable.variable_id.toLowerCase» + "\">\n" +
						    «IF (!(logic.rowCreationApproachForCube.rowCreationApproach instanceof GroupByRowCreationApproach) && !(logic.rowCreationApproachForCube.rowCreationApproach instanceof FilterAndGroupToOneRowCreationApproach))»
						    «IF column instanceof StandardBasicColumnFunction»
						    «val basicfunction = column.basicFunction»
						    «basicfunction.getXMLForBasicFunctionParameterisedBySourceRowID()»
						     «ENDIF»
						     «ENDIF»
						     "   </formulaCells>\n"
						     «ENDIF»««««IF !column.usedInTrails»""«ENDIF»
						  «ENDFOR»
						
						   «FOR column:logic.columnFunctionGroup.columnFunctions SEPARATOR "+"»«column.variable.variable_id.toLowerCase»Text«ENDFOR»
					   
						} 
						«ENDIF»
				«ENDIF»
				«ENDFOR»
		
				
				def stringForEndOfXML() : String = {			
				  "</data_lineage:DataLineageModel>\n"
				}
		
		}
	 '''
	
	/**
	 * Add backslashes
	 */
	def String addbackslashes(String string) {
		 string.replace("\\","\\\\");
		
	}
	
	/**
	 * Get the user CubeTransformationLogic objects form the AttributeLineageUtil in the 
	 * correct order so that dependencies always come before the FunctionalRowLogics 
	 * objects that they are dependent upon.
	 */
	def EList<CubeTransformationLogic> getOrderedRowTransformations(AttributeLineageModel program)
	{
		val orderedList =  AttributeLineageUtil.getOrderedRowTransformations(program)			
		return orderedList;
				
	}
	
	
	
	
	
	/**
	 * get the XML For AggregateFunctions Parameterised By SourceRowID
	 */
	def String getXMLForAggregateFunctionParameterisedBySourceRowID(AggregateFunction function, String logicName)
	{
		  val List<String> l = new ArrayList<String>()
		  val uri = function.functionSpec.eResource.URI
		  val lastSegmentIndex = uri.segmentCount -1
		  val lastSegment =uri.segment(lastSegmentIndex)
		  val secondLastSegment = uri.segment(lastSegmentIndex -1)
		  val prefix = "..\\\\"+secondLastSegment+"\\\\" + lastSegment
		  
		
		 l.add("\"   <function xsi:type=\\\"functions:AggregateFunction\\\" functionSpec=\\\"" + prefix+ "#"+ function.functionSpec.name + "\\\">\\n\" + " )
		 val params = function.parameters   
		 params.forEach[
		 	if(it instanceof ResolvedCubeColumnParameter)
		 	l.add("getParamsForTheSourceRowsOf" +logicName + "(row,\"" +it.cubeColumn.variable.variable_id + "\") +")
		 	 	
		 ]   
		    
		    
		     
		 l.add("    \"   </function>\\n\" +  ")
		     
		 return l.fold("",[ a, b |   a+b ])

	}
	/**
	 * get the XML For BasicFunctions Parameterised By SourceRowID
	 */
	def String getXMLForBasicFunctionParameterisedBySourceRowID(BasicFunction function)
	{
		 val List<String> l = new ArrayList<String>()
	
		  val uri = function.functionSpec.eResource.URI
		  val lastSegmentIndex = uri.segmentCount -1
		  val lastSegment =uri.segment(lastSegmentIndex)
		  val secondLastSegment = uri.segment(lastSegmentIndex -1)
		  val prefix = "..\\\\"+secondLastSegment+"\\\\" + lastSegment
		  
		 
		
		 val funcName = function.functionSpec.name 
		 l.add("\"   <function xsi:type=\\\"functions:BasicFunction\\\" functionSpec=\\\"" + prefix + "#"+ function.functionSpec.name + "\\\">\\n\" + \n" )
		 val params = function.parameters   
		 params.forEach[
		 	if(it instanceof ResolvedCubeColumnParameter)
			 	if(!funcName.equals("CopyColumn"))   
			 		l.add("\"     <parameters xsi:type=\\\"functions:CellsParameter\\\" cells=\\\"#\" + sourcerowID" + it.cubeColumn.cube.cube_name + " +\":" + it.cubeColumn.variable.variable_id +"\\\"/>\\n\" + \n" )
			 	else if (l.size == 1)
			 	  	l.add("\"     <parameters xsi:type=\\\"functions:CellsParameter\\\" cells=\\\"#\" + sourcerowID" + it.cubeColumn.cube.cube_name + " +\":" + it.cubeColumn.variable.variable_id +"\\\"/>\\n\" + \n" )
			 	
		 ]   
		    
		    
		     
		 l.add("    \"   </function>\\n\" +  ")
		     
		 return l.fold("",[ a, b |   a+b ])

	}
	
	
	
	def generateScalaCodeForTableData(BaseCubeData data) '''
	package «data.cube.cube_name»_data
	
	
	«IF data.cube instanceof BaseDeltaCube»
		«val deltaCubeName = (data.cube as BaseDeltaCube).ultimateBaseCube.cube_name»
		import base_types._ 
			import «deltaCubeName»_def._
			
			object  «data.cube.cube_name»Data {
				
			
			def initialiseRowList ()  :List[RRow«deltaCubeName»]  = {
			
			  val rowlist : List[RRow«deltaCubeName»] = List.empty[RRow«deltaCubeName»] 
			  «FOR a:data.rows» 
			     val «a.rowID» : RRow«deltaCubeName» = RRow«deltaCubeName»( "«a.rowIDToDelete»" , «IF a.isDelete»true«ELSE»false «ENDIF», "«a.rowID»" , «FOR b:a.cells SEPARATOR ','» «IF b instanceof BaseCellWithEnumeratedValue»"«b.value.name»"«ENDIF»«IF b instanceof BaseCellWithValue && (b.column.domain_id.data_type == FACET_VALUE_TYPE.STRING)»"«b.value»"«ENDIF»«IF b instanceof BaseCellWithValue && (b.column.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)»«b.value»«ENDIF»«ENDFOR»)
			«ENDFOR»
				 return  rowlist  «FOR a:data.rows» :+ «a.rowID»«ENDFOR»
				 }
				}
	«ELSE»
	import base_types._ 
	import «data.cube.cube_name»_def._
	
	object  «data.cube.cube_name»Data {
		
	
	def initialiseRowList ()  :List[RRow«data.cube.cube_name»]  = {
	
  val rowlist : List[RRow«data.cube.cube_name»] = List.empty[RRow«data.cube.cube_name»] 
	  «FOR a:data.rows» 
	     val «a.rowID» : RRow«data.cube.cube_name» = RRow«data.cube.cube_name»( "«a.rowIDToDelete»" ,«IF a.isDelete»true«ELSE»false «ENDIF» , "«a.rowID»" , «FOR b:a.cells SEPARATOR ','» «IF b instanceof BaseCellWithEnumeratedValue»"«b.value.name»"«ENDIF»«IF b instanceof BaseCellWithValue && (b.column.domain_id.data_type == FACET_VALUE_TYPE.STRING)»"«b.value»"«ENDIF»«IF b instanceof BaseCellWithValue && (b.column.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)»«b.value»«ENDIF»«ENDFOR»)
	
	«ENDFOR»
	 return  rowlist  «FOR a:data.rows» :+ «a.rowID»«ENDFOR»
	}
	}
	«ENDIF»
	
'''
	/**
	 * get the value of a cell
	 */
	def String getValue(BaseCell cell)
	{
		if (cell instanceof BaseCellWithEnumeratedValue)
		{
			return "\"" + (cell as BaseCellWithEnumeratedValue).value.member_id + "\"";
		}
		if (cell instanceof BaseCellWithValue)
		{
			return (cell as BaseCellWithValue).value.toString;
		}
		
	}
	
	/**
	 * generate SourceSchema Scala code
	 */
	def generateSourceSchemaScalaCode(BaseRowStructure schema) '''
	package «schema.cube.cube_name»_def
	import base_types._ 
	
	«IF !(schema.cube instanceof BaseDeltaCube)»
		
		case class RTable«schema.cube.cube_name»(rl : List[RRow«schema.cube.cube_name»])  extends RTable 
		case class RRow«schema.cube.cube_name» ( rowIDToDelete  : String, isDelete : Boolean, conciseid : String, «FOR c:schema.columns SEPARATOR ','»«c.variable.variable_id.toLowerCase» « IF (!c.variable.domain_id.isIs_enumerated && c.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!c.variable.domain_id.isIs_enumerated && c.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF c.variable.domain_id.isIs_enumerated » :String   «ENDIF» «ENDFOR») extends RRow	
			
		object  «schema.cube.cube_name»Def {	
		def getRRows«schema.cube.cube_name»( t: RTable«schema.cube.cube_name») : List[RRow«schema.cube.cube_name»] = t match {
					      case RTable«schema.cube.cube_name»(rl) => rl
					  
					  }
			  «FOR a:schema.columns»
			 
			   
			  def get«a.variable.variable_id.toLowerCase»(r : RRow«schema.cube.cube_name»)«IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF a.variable.domain_id.isIs_enumerated » :String   «ENDIF» = r match {
			      case RRow«schema.cube.cube_name»(rowIDToDelete  : String, isDelete : Boolean, conciseid : String, «FOR b:schema.columns SEPARATOR ','»«b.variable.variable_id.toLowerCase» «IF (!b.variable.domain_id.isIs_enumerated && b.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!b.variable.domain_id.isIs_enumerated && b.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF b.variable.domain_id.isIs_enumerated » :String   «ENDIF»  «ENDFOR» ) => «a.variable.variable_id.toLowerCase»
			  }
					  		
			«ENDFOR» 
			
			  def getconciseid(r : RRow«schema.cube.cube_name») :String  = r match { 
			 		case RRow«schema.cube.cube_name»( rowIDToDelete  : String, isDelete : Boolean, conciseid : String, «FOR b:schema.columns SEPARATOR ','»«b.variable.variable_id.toLowerCase» «IF (!b.variable.domain_id.isIs_enumerated && b.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!b.variable.domain_id.isIs_enumerated && b.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF b.variable.domain_id.isIs_enumerated » :String   «ENDIF»  «ENDFOR» ) =>  conciseid : String
			 		  }	
			 
		
		}
		«ENDIF»
		«IF (schema.cube instanceof BaseDeltaCube)»
			«val ultimateSourceName= (schema.cube as BaseDeltaCube).ultimateBaseCube.cube_name»
			import 	«ultimateSourceName»_def._ 
			case class RTable«schema.cube.cube_name»(rl : List[RRow«ultimateSourceName»])  extends RTable 
			object  «schema.cube.cube_name»Def {	
			def getRRows«schema.cube.cube_name»( t: RTable«schema.cube.cube_name») : List[RRow«ultimateSourceName»] = t match {
							      case RTable«schema.cube.cube_name»(rl) => rl
							  
							  }
			}
			«ENDIF»
		
'''
	/**
	 * generate Scala Code For RowLogic
	 */
	def generateScalaCodeForRowLogic(CubeTransformationLogic logic,String logicTableName,List<String> sourceTableNames, List<FreeBirdToolsCube> sourceCubes) '''
	package «logic.rowCreationApproachForCube.cube.cube_name»_def
	import base_types._ 
	import funcdefs._ 
	import customfuncs._
	«val sourceTableName = sourceTableNames.get(0)»
	«val sourceCube = sourceCubes.get(0)»
	«IF logic.rowCreationApproachForCube.rowCreationApproach instanceof DeltaAccumulationRowFunction»
			«FOR stn:sourceTableNames»
			import «stn»_def._  //import each source table for this table
			«ENDFOR»
			«val ultimateSourceName= (logic.rowCreationApproachForCube.rowCreationApproach as DeltaAccumulationRowFunction).ultimateCubeSource.cube_name»
			import «ultimateSourceName»_def._  //imprt ultimate source table 
				
			case class RTable«logicTableName»(«FOR stn:sourceTableNames SEPARATOR ','»«stn»: RTable«stn»«ENDFOR») extends RTable
			//case class RRow«logicTableName»(«FOR stn:sourceTableNames SEPARATOR ','»«stn»: RRow«stn»«ENDFOR») 
			object «logicTableName»Def  {
			def getRRows«logicTableName»(table«logicTableName» : RTable«logicTableName») : List[RRow«ultimateSourceName»] = table«logicTableName» match {
			      case RTable«logicTableName»(«FOR stn:sourceTableNames SEPARATOR ','»table«stn»«ENDFOR») => getRRows«logicTableName»(«FOR stn:sourceTableNames SEPARATOR ','»«stn»Def.getRRows«stn»(table«stn»)«ENDFOR»)
			  }
				  
			 	def getRRows«logicTableName»(rows«sourceTableNames.get(0)»: List[RRow«ultimateSourceName»]«IF sourceTableNames.size > 1», rows«sourceTableNames.get(1)»: List[RRow«ultimateSourceName»] «ENDIF» ) : List[RRow«ultimateSourceName»]=  {
			     
			     «IF sourceTableNames.size > 1»
			     // if we have an add of something and exists in the previous, then we just want the new add.
			     // so we can add everything from the real base table, and then everything from the derived 
			     // detla dtable unless there is an equivalent in the base.
			     
			     rows«sourceTableNames.get(1)» ::: filterAdds(rows«sourceTableNames.get(0)», rows«sourceTableNames.get(1)»)
			     
			     «ELSE»
			     rows«sourceTableNames.get(0)»
			     «ENDIF»
			     //so here we are replacing adds, and propegating deltas
			    
			  }
			   «IF sourceTableNames.size > 1»
			def filterAdds(rows«sourceTableNames.get(0)» : List[RRow«ultimateSourceName»] , rows«sourceTableNames.get(1)» : List[RRow«ultimateSourceName»] ) : List[RRow«ultimateSourceName»] =  rows«sourceTableNames.get(0)» match
			{
				case  tail :+ head => emptyListIfTailIstoBeRemoved(head,rows«sourceTableNames.get(1)») :::   filterAdds(tail,rows«sourceTableNames.get(1)») 
			       case List() => List.empty[RRow«ultimateSourceName»]
				//can we just filter each in turn and append
			}
			
			def emptyListIfTailIstoBeRemoved(item : RRow«ultimateSourceName» , listToCheck : List[RRow«ultimateSourceName»] ) : List[RRow«ultimateSourceName»] =  listToCheck match
			{
				case  tail :+ head => if (head.rowIDToDelete.equals(item.conciseid) && !head.isDelete) List.empty[RRow«ultimateSourceName»]  else emptyListIfTailIstoBeRemoved(item,tail)
			       case List() => List.empty[RRow«ultimateSourceName»] :+  item
				//can we just filter each in turn and append
			}
			   «ENDIF»
			«ENDIF»	
			
		«IF logic.rowCreationApproachForCube.rowCreationApproach instanceof BaseViewIncorporatingDeltasRowFunction»
				«FOR stn:sourceTableNames»
				import «stn»_def._  //import each source table for this table
				«ENDFOR»
				«val ultimateSourceName= (logic.rowCreationApproachForCube.rowCreationApproach as BaseViewIncorporatingDeltasRowFunction).ultimateCubeSource.cube_name»
				import 	«ultimateSourceName»_def._ 
				case class RTable«logicTableName»(«FOR stn:sourceTableNames SEPARATOR ','»«stn»: RTable«stn»«ENDFOR») extends RTable
				case class RRow«logicTableName»(«ultimateSourceName»: RRow«ultimateSourceName») 
				object «logicTableName»Def  {
				def getRRows«logicTableName»(table«logicTableName» : RTable«logicTableName») : List[RRow«logicTableName»] = table«logicTableName» match {
				      case RTable«logicTableName»(«FOR stn:sourceTableNames SEPARATOR ','»table«stn»«ENDFOR») => getRRows«logicTableName»(«FOR stn:sourceTableNames SEPARATOR ','»«stn»Def.getRRows«stn»(table«stn»)«ENDFOR»)
				  }
					def getRRows«logicTableName»(«sourceTableNames.get(0)»: List[RRow«ultimateSourceName»]«IF sourceTableNames.size > 0», «sourceTableNames.get(1)»: List[RRow«ultimateSourceName»] «ENDIF» ) : List[RRow«logicTableName»]=  {
				  «IF sourceTableNames.size > 1»
				 		     // if we have an add of something and exists in the previous, then we just want the new add.
				 		     // so we can add everything from the real base table, and then everything from the derived 
				 		     // detla dtable unless there is an equivalent in the base.
				 		     
				 		     makeNewListWithSources(everythingExceptDeletes(«sourceTableNames.get(0)») ::: filterAddsAndDeletes( «ultimateSourceName»,«sourceTableNames.get(0)»))
				 		     «ELSE»
				 		     rows«ultimateSourceName»
				 		     «ENDIF»
				 		     //so here we are replacing adds, and aplying deltas
				 		    
				 		  }
				 		   «IF sourceTableNames.size > 1»
				 		   //all items from table 1 as long as they are not amended or delted in table 2
				 		def filterAddsAndDeletes(rows«ultimateSourceName» : List[RRow«ultimateSourceName»] , rows«sourceTableNames.get(0)» : List[RRow«ultimateSourceName»] ) : List[RRow«ultimateSourceName»] =  rows«ultimateSourceName» match
				 		{
				 			case  tail :+ head => emptyListIfTailIstoBeRemoved(head,rows«sourceTableNames.get(0)») :::   filterAddsAndDeletes(tail,rows«sourceTableNames.get(0)») 
				 		       case List() => List.empty[RRow«ultimateSourceName»]
				 			//can we just filter each in turn and append
				 		}
				 		
				 		def everythingExceptDeletes(rows«sourceTableNames.get(0)» : List[RRow«ultimateSourceName»]  ) : List[RRow«ultimateSourceName»] =  rows«sourceTableNames.get(0)» match
				 					 		{
				 					 			case  tail :+ head => (if (head.isDelete) List.empty[RRow«ultimateSourceName»]  else (List.empty[RRow«ultimateSourceName»] :+head ) )  ::: everythingExceptDeletes(tail)
				 					 		       case List() => List.empty[RRow«ultimateSourceName»]
				 					 			//can we just filter each in turn and append
				 					 		}
				 					 		
				 		def emptyListIfTailIstoBeRemoved(item : RRow«ultimateSourceName» , listToCheck : List[RRow«ultimateSourceName»] ) : List[RRow«ultimateSourceName»] =  listToCheck match
				 		{
				 			case  tail :+ head => if (head.rowIDToDelete.equals(item.conciseid)) List.empty[RRow«ultimateSourceName»]  else emptyListIfTailIstoBeRemoved(item,tail)
				 		       case List() => List.empty[RRow«ultimateSourceName»] :+  item
				 			//can we just filter each in turn and append
				 		}
				 		
				 		def makeNewListWithSources(list : List[RRow«ultimateSourceName»] )  : List[RRow«logicTableName»] =  list match
				 		{
				 			case  tail :+ head =>  makeNewListWithSources(tail) :+  RRow«logicTableName»(head)
				 						 		       case List() => List.empty[RRow«logicTableName»] 
				 			
				 		} «ENDIF»	
				  //for each col in the schema get the same col from the undelrying row
				  ///so ge the schema, is there a function on Util to do this?
				  « val schema = logic.getTheDependantEvaluatedCubeSchemas.get(0)»
			    «FOR a:schema.columns»	  		 
				  		   
				  		  def get«a.variable.variable_id.toLowerCase»(r : RRow«logicTableName»)«IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF a.variable.domain_id.isIs_enumerated » :String   «ENDIF» = r match {
				  		      case RRow«logicTableName»(sr:RRow«schema.cube.cube_name»)  => «schema.cube.cube_name»Def.get«a.variable.variable_id.toLowerCase»(sr)
				  		  }
				  		
				  		«ENDFOR» 
				  		
				  		def gettracingid(r : RRow«logicTableName») :String  =  {
				  			 						     "«logicTableName»(" + «IF ultimateSourceName !=null»«ultimateSourceName»«ELSE»«sourceTableName»«ENDIF»Def.getconciseid(r.«ultimateSourceName») + ")"
				  			 						  }
				  		def getconciseid(r : RRow«logicTableName») :String  = {
				  			 			 						     "«logicTableName»:" +md5HashString("«logicTableName»(" + «IF ultimateSourceName !=null»«ultimateSourceName»«ELSE»«sourceTableName»«ENDIF»Def.getconciseid(r.«ultimateSourceName») + ")")
				  			 			 						  }
	
				«ENDIF»	
	
		
			
	«IF logic.rowCreationApproachForCube.rowCreationApproach instanceof OneToOneRowCreationApproach»

	
	import «sourceTableName»_def._  //import each source table for this table


	case class RTable«logicTableName»(t1: RTable«sourceTableName») extends RTable 
	
	
	case class RRow«logicTableName»(«sourceTableName»: RRow«sourceTableName») 

	object «logicTableName»Def  {
	   
	  def getRRows«logicTableName»(t : RTable«logicTableName») : List[RRow«logicTableName»]= t match {
	      case RTable«logicTableName»(someTable) => getRRows«logicTableName»(«sourceTableName»Def.getRRows«sourceTableName»(someTable))
	  }
	  
	  def getRRows«logicTableName»( rl: List[RRow«sourceTableName»]) : List[RRow«logicTableName»] = rl match {
	       case  tail :+ head => getRRows«logicTableName»(tail) :+   RRow«logicTableName»(head) 
	       case List() => List.empty[RRow«logicTableName»]  
	  }
	
	 //now for each column get the logic
	 		  «FOR a:logic.columnFunctionGroup.columnFunctions»
	 		  «IF a instanceof StandardBasicColumnFunction»
	 		 def get«a.variable.variable_id.toLowerCase»(r : RRow«logicTableName»)«IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF a.variable.domain_id.isIs_enumerated » :String   «ENDIF» = r match {  //set the type based on dim, id, measure
	 		 case RRow«logicTableName»(«sourceTableName») => «a.getScalaFunctionString()» //get the spec from function, check infix, and repalce tablecolumns appropriatelty with function calls.
	 			  } 
	 		  «ENDIF»
	 		  
	 		«ENDFOR» 
	 		
	 		def gettracingid(r : RRow«logicTableName») :String  =  {
	 						     "«logicTableName»(" + «sourceTableName»Def.getconciseid(r.«sourceTableName») + ")"
	 						  }
	 		def getconciseid(r : RRow«logicTableName») :String  = {
	 			 						     "«logicTableName»:" +md5HashString("«logicTableName»(" + «sourceTableName»Def.getconciseid(r.«sourceTableName») + ")")
	 			 						  }
		
		
	«ENDIF»
	«IF logic.rowCreationApproachForCube.rowCreationApproach instanceof UnionRowCreationApproach»
	
		
		import «sourceTableName»_def._  //import each source table for this table
	
	
		case class RTable«logicTableName»(t1: RTable«sourceTableName») extends RTable 
		
		
		case class RRow«logicTableName»(«sourceTableName»: RRow«sourceTableName») 
	
		object «logicTableName»Def  {
		   
		  def getRRows«logicTableName»(t : RTable«logicTableName») : List[RRow«logicTableName»]= t match {
		      case RTable«logicTableName»(someTable) => getRRows«logicTableName»(«sourceTableName»Def.getRRows«sourceTableName»(someTable))
		  }
		  
		  def getRRows«logicTableName»( rl: List[RRow«sourceTableName»]) : List[RRow«logicTableName»] = rl match {
		       case  tail :+ head => getRRows«logicTableName»(tail) :+   RRow«logicTableName»(head) 
		       case List() => List.empty[RRow«logicTableName»]  
		  }
		
		 //now for each column get the logic
		 		  «FOR a:logic.columnFunctionGroup.columnFunctions»
		 		  «IF a instanceof StandardBasicColumnFunction»
		 		 def get«a.variable.variable_id.toLowerCase»(r : RRow«logicTableName»)«IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF a.variable.domain_id.isIs_enumerated » :String   «ENDIF» = r match {  //set the type based on dim, id, measure
		 		 case RRow«logicTableName»(«sourceTableName») => «a.getScalaFunctionString()» //get the spec from function, check infix, and repalce tablecolumns appropriatelty with function calls.
		 			  } 
		 		  «ENDIF»
		 		
		 		«ENDFOR» 
		 		
		 		def gettracingid(r : RRow«logicTableName») :String  =  {
		 						     "«logicTableName»(" + «sourceTableName»Def.getconciseid(r.«sourceTableName») + ")"
		 						  }
		 		def getconciseid(r : RRow«logicTableName») :String  = {
		 			 						     "«logicTableName»:" +md5HashString("«logicTableName»(" + «sourceTableName»Def.getconciseid(r.«sourceTableName») + ")")
		 			 						  }
			
			
		«ENDIF»
	
	«IF logic.rowCreationApproachForCube.rowCreationApproach instanceof RowJoinFunction»
	«FOR stn:sourceTableNames»
	import «stn»_def._  //imprt each source table for this table
	«ENDFOR»
		
	case class RTable«logicTableName»(«FOR stn:sourceTableNames SEPARATOR ','»«stn»: RTable«stn»«ENDFOR») extends RTable
	case class RRow«logicTableName»(«FOR stn:sourceTableNames SEPARATOR ','»«stn»: RRow«stn»«ENDFOR») 
	object «logicTableName»Def  {
	def getRRows«logicTableName»(table«logicTableName» : RTable«logicTableName») : List[RRow«logicTableName»] = table«logicTableName» match {
	      case RTable«logicTableName»(«FOR stn:sourceTableNames SEPARATOR ','»table«stn»«ENDFOR») => getRRows«logicTableName»(«FOR stn:sourceTableNames SEPARATOR ','»«stn»Def.getRRows«stn»(table«stn»)«ENDFOR»)
	  }
		  
	 def getRRows«logicTableName»(«FOR stn:sourceTableNames SEPARATOR ','»row«stn»List: List[RRow«stn»]«ENDFOR»): List[RRow«logicTableName»] =  {
	      (for{
	      	«FOR stn:sourceTableNames»
	     «stn» <- row«stn»List
«ENDFOR»
	     if («(logic.rowCreationApproachForCube.rowCreationApproach as RowJoinFunction).joinFunction.getScalaFunctionString»)
	   } yield RRow«logicTableName»(«FOR stn:sourceTableNames SEPARATOR ','»«stn»«ENDFOR»))
	  }
	
	
		  //now for each column get the logic
		  «FOR a:logic.columnFunctionGroup.columnFunctions»
		  «IF a instanceof StandardBasicColumnFunction»
		 def get«a.variable.variable_id.toLowerCase»(r : RRow«logicTableName»)«IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF a.variable.domain_id.isIs_enumerated » :String   «ENDIF»  = r match {  //set the type based on dim, id, measure
		 case RRow«logicTableName»(«FOR stn:sourceTableNames SEPARATOR ','»«stn»«ENDFOR») => «a.getScalaFunctionString()» //get the spec from function, check infix, and repalce tablecolumns appropriatelty with function calls.
			  } 
		  «ENDIF»
		
		«ENDFOR» 
		
		def gettracingid(r : RRow«logicTableName») :String  =  {
				 "«logicTableName»(" + «sourceTableNames.get(0)»Def.getconciseid(r.«sourceTableNames.get(0)») + ":" + «sourceTableNames.get(1)»Def.getconciseid(r.«sourceTableNames.get(1)») +")"
							 						  }
		def getconciseid(r : RRow«logicTableName») :String  =  {
				"«logicTableName»:" +md5HashString("«logicTableName»(" + «sourceTableNames.get(0)»Def.getconciseid(r.«sourceTableNames.get(0)») + ":" + «sourceTableNames.get(1)»Def.getconciseid(r.«sourceTableNames.get(1)») +")")
					 						  }
	«ENDIF»	
	
	«IF logic.rowCreationApproachForCube.rowCreationApproach instanceof GroupByRowCreationApproach»
		«val groupbycol1 = ((logic.rowCreationApproachForCube.rowCreationApproach) as GroupByRowCreationApproach).groupByColumns.get(0).variable_id.toLowerCase»
		
		
		import «sourceTableName»_def._  //imprt each source table for this table

		case class RTable«logicTableName»(t1: RTable«sourceTableName») extends RTable
		case class RRow«logicTableName»(«sourceTableName»: List[RRow«sourceTableName»])
		
		object «logicTableName»Def  {	
		  def getRRows«logicTableName»(t : RTable«logicTableName») : List[RRow«logicTableName»] = t match {
		      case RTable«logicTableName»(table«sourceTableName») => getRRows«logicTableName»(«sourceTableName»Def.getRRows«sourceTableName»(table«sourceTableName»))
		  }
		
		  
		  def getRRows«logicTableName»( rl: List[RRow«sourceTableName»]) : List[RRow«logicTableName»] = rl match {
		      case _ =>  getRRow«logicTableName»s2 (getRRow«logicTableName»Maps(rl), convertSetToList(getRRow«logicTableName»Maps(rl).keySet) )
		  }
		    
		
		    
		  def getRRow«logicTableName»s2 (ml: Map[(«FOR gbc:((logic.rowCreationApproachForCube.rowCreationApproach) as GroupByRowCreationApproach).groupByColumns SEPARATOR ','»String«ENDFOR»),List[RRow«sourceTableName»]] , keyset : List [(«FOR gbc:((logic.rowCreationApproachForCube.rowCreationApproach) as GroupByRowCreationApproach).groupByColumns SEPARATOR ','»String«ENDFOR»)]) : List[RRow«logicTableName»] = keyset match {   
		   case tail :+  head  =>  getRRow«logicTableName»s2 (ml,tail) :+ RRow«logicTableName»(ml(head))
		   case List() => List.empty[RRow«logicTableName»] 
		      
		 }
		  
		  def convertSetToList (xs : Set[(«FOR gbc:((logic.rowCreationApproachForCube.rowCreationApproach) as GroupByRowCreationApproach).groupByColumns SEPARATOR ','»String«ENDFOR»)]) : List[(«FOR gbc:((logic.rowCreationApproachForCube.rowCreationApproach) as GroupByRowCreationApproach).groupByColumns SEPARATOR ','»String«ENDFOR»)] = { 
		    
		     val ys:  List[(«FOR gbc:((logic.rowCreationApproachForCube.rowCreationApproach) as GroupByRowCreationApproach).groupByColumns SEPARATOR ','»String«ENDFOR»)] = (for(x <- xs) yield  x)(collection.breakOut)
		     return ys
		  }
		  
		  def  getRRow«logicTableName»Maps( rl: List[RRow«sourceTableName»]) : Map[(«FOR gbc:((logic.rowCreationApproachForCube.rowCreationApproach) as GroupByRowCreationApproach).groupByColumns SEPARATOR ','»String«ENDFOR»),List[RRow«sourceTableName»]]  = rl match {
		    case record =>  rl.groupBy(record => («FOR gbc:((logic.rowCreationApproachForCube.rowCreationApproach) as GroupByRowCreationApproach).groupByColumns SEPARATOR ','»«sourceTableName»Def.get«gbc.variable_id.toLowerCase»(record)«ENDFOR» ) )
		      
		  } 
		  
		    «FOR a:logic.columnFunctionGroup.columnFunctions»
		     «IF a instanceof AggregateColumnFunction»
		     «IF a.aggregateFunction.functionSpec.name.equals("SUM")»
		     
		      def get«a.variable.variable_id.toLowerCase»(r : RRow«logicTableName») «IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF a.variable.domain_id.isIs_enumerated » :String   «ENDIF» = r match {  
		        case RRow«logicTableName»(list) => get«a.variable.variable_id.toLowerCase»2(list)		       
		    } 
		    
		    def get«a.variable.variable_id.toLowerCase»2(r1: List[RRow«sourceTableName»]) «IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF a.variable.domain_id.isIs_enumerated » :String   «ENDIF» = r1 match {  
		        case tail :+  head  =>  get«a.variable.variable_id.toLowerCase»2(tail) + «sourceTableName»Def.get«a.getRequiredColumn.variable_id.toLowerCase»(head)  
		         case List() => 0 
		    }
		    «ENDIF»	
		      «IF a.aggregateFunction.functionSpec.name.equals("First")»
		      def get«a.variable.variable_id.toLowerCase»(r : RRow«logicTableName») «IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF a.variable.domain_id.isIs_enumerated » :String   «ENDIF» = r match {  
		        case RRow«logicTableName»(list) => get«a.variable.variable_id.toLowerCase»2(list)		       
		    } 
		    
		    def get«a.variable.variable_id.toLowerCase»2(r1: List[RRow«sourceTableName»]) «IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF a.variable.domain_id.isIs_enumerated » :String   «ENDIF» = r1 match {  
		        case tail :+  head  => «sourceTableName»Def.get«a.getRequiredColumn.variable_id.toLowerCase»(head)  
		         case List() =>""
		    }

			«ENDIF»	
			«ENDIF»	
			«ENDFOR»
			def gettracingid(r : RRow«logicTableName») :String  = r match {
				  case RRow«logicTableName»(list) => "«logicTableName»(" + gettracingid2(list) + ")"	
			}
			
			def gettracingid2(r1: List[RRow«sourceTableName»]) :String = r1 match {  
				   case List() :+  head  =>  «sourceTableName»Def.getconciseid(head)
				   case tail :+  head  => gettracingid2(tail) + ":" +  «sourceTableName»Def.getconciseid(head)  
				    
				 		         case List() =>""
			}						  
			
			def getconciseid(r : RRow«logicTableName») :String  = r match {
							  case RRow«logicTableName»(list) => "«logicTableName»:" +md5HashString("«logicTableName»(" + getconciseid2(list) + ")")	
						}
						
			def getconciseid2(r1: List[RRow«sourceTableName»]) :String = r1 match {  
							    case List() :+  head  =>  «sourceTableName»Def.getconciseid(head)
							   case tail :+  head  =>   «sourceTableName»Def.getconciseid(head) + ":" + gettracingid2(tail) 
							 		         case List() =>""
						}
 				
	«ENDIF»	
	«IF logic.rowCreationApproachForCube.rowCreationApproach instanceof FilterRowCreationApproach»
		import «sourceTableName»_def._  //imprt each source table for this table
		
		
		case class RTable«logicTableName»(t1: RTable«sourceTableName») extends RTable 
		case class RRow«logicTableName»(«sourceTableName»: RRow«sourceTableName») 
		
		object «logicTableName»Def  {
		   
		  def getRRows«logicTableName»(t : RTable«logicTableName») : List[RRow«logicTableName»]= t match {
		      case RTable«logicTableName»(someTable) => getRRows«logicTableName»(«sourceTableName»Def.getRRows«sourceTableName»(someTable))
		  }
		  
		  def getRRows«logicTableName»( rl: List[RRow«sourceTableName»]) : List[RRow«logicTableName»] = rl match {
		       case  tail :+ «sourceTableName» => if («(logic.rowCreationApproachForCube.rowCreationApproach as FilterRowCreationApproach).filterFunction.getScalaFunctionString») {getRRows«logicTableName»(tail) :+   RRow«logicTableName»(«sourceTableName») } else getRRows«logicTableName»(tail)
		       case List() => List.empty[RRow«logicTableName»]  
		  }
		
		 //now for each column get the logic
		 		  «FOR a:logic.columnFunctionGroup.columnFunctions»
		 		  «IF a instanceof StandardBasicColumnFunction»
		 		 def get«a.variable.variable_id.toLowerCase»(r : RRow«logicTableName»)«IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF a.variable.domain_id.isIs_enumerated » :String   «ENDIF»  = r match {  //set the type based on dim, id, measure
		 		 case RRow«logicTableName»(«FOR stn:sourceTableNames SEPARATOR ','»«stn»«ENDFOR») => «a.getScalaFunctionString()» //get the spec from function, check infix, and repalce tablecolumns appropriatelty with function calls.
		 			  } 
		 		  «ENDIF»
		 		 
		 		   
		 		     «IF a instanceof AggregateColumnFunction»
		 		   		     «IF a.aggregateFunction.functionSpec.name.equals("SUM")»
		 		   		     
		 		   		      def get«a.variable.variable_id.toLowerCase»(r : RRow«logicTableName») «IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF a.variable.domain_id.isIs_enumerated » :String   «ENDIF» = r match {  
		 		   		        case RRow«logicTableName»(list) => get«a.variable.variable_id.toLowerCase»2(list)		       
		 		   		    } 
		 		   		    
		 		   		    def get«a.variable.variable_id.toLowerCase»2(r1: List[RRow«sourceTableName»]) «IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF a.variable.domain_id.isIs_enumerated » :String   «ENDIF» = r1 match {  
		 		   		        case tail :+  head  =>  get«a.variable.variable_id.toLowerCase»2(tail) + «sourceTableName»Def.get«a.getRequiredColumn.variable_id.toLowerCase»(head)  
		 		   		         case List() => 0 
		 		   		    }
		 		   		    «ENDIF»	
		 		   		      «IF a.aggregateFunction.functionSpec.name.equals("First")»
		 		   		      def get«a.variable.variable_id.toLowerCase»(r : RRow«logicTableName») «IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF a.variable.domain_id.isIs_enumerated » :String   «ENDIF» = r match {  
		 		   		        case RRow«logicTableName»(list) => get«a.variable.variable_id.toLowerCase»2(list)		       
		 		   		    } 
		 		   		    
		 		   		    def get«a.variable.variable_id.toLowerCase»2(r1: List[RRow«sourceTableName»]) «IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF a.variable.domain_id.isIs_enumerated » :String   «ENDIF» = r1 match {  
		 		   		        case tail :+  head  => «sourceTableName»Def.get«a.getRequiredColumn.variable_id.toLowerCase»(head)  
		 		   		         case List() =>""
		 		   		    }
		 		   
		 		   			«ENDIF»	
		 		   			«ENDIF»	
		 		   
		 		   
		 		«ENDFOR» 
			
				 		def gettracingid(r : RRow«logicTableName») :String  =  {
				 						     "«logicTableName»(" + «sourceTableName»Def.getconciseid(r.«sourceTableName») + ")"
				 						  }
				 		def getconciseid(r : RRow«logicTableName») :String  =  {
				 			 						     "«logicTableName»:" +md5HashString("«logicTableName»(" + «sourceTableName»Def.getconciseid(r.«sourceTableName») + ")")
				 			 						  }
					
					
			
		«ENDIF»
		
			«IF logic.rowCreationApproachForCube.rowCreationApproach instanceof FilterAndGroupToOneRowCreationApproach»
				import «sourceTableName»_def._  //imprt each source table for this table
				
				case class RTable«logicTableName»(t1: RTable«sourceTableName») extends RTable
						case class RRow«logicTableName»(«sourceTableName»: List[RRow«sourceTableName»])
						
						object «logicTableName»Def  {	
						  def getRRows«logicTableName»(t : RTable«logicTableName») : List[RRow«logicTableName»] = t match {
						      case RTable«logicTableName»(table«sourceTableName») => getRRows«logicTableName»(«sourceTableName»Def.getRRows«sourceTableName»(table«sourceTableName»))
						  }
						
						  
						  def getRRows«logicTableName»( rl: List[RRow«sourceTableName»]) : List[RRow«logicTableName»] = rl match {
						      case _ =>  getRRow«logicTableName»s2 (getRRow«logicTableName»Maps(getFiltered«sourceTableName»(rl)), convertSetToList(getRRow«logicTableName»Maps(getFiltered«sourceTableName»(rl)).keySet) )
						  }
						    
						
						    
						  def getRRow«logicTableName»s2 (ml: Map[(«FOR gbc:((logic.rowCreationApproachForCube.rowCreationApproach) as FilterAndGroupToOneRowCreationApproach).groupByVariables SEPARATOR ','»String«ENDFOR»),List[RRow«sourceTableName»]] , keyset : List [(«FOR gbc:((logic.rowCreationApproachForCube.rowCreationApproach) as FilterAndGroupToOneRowCreationApproach).groupByVariables SEPARATOR ','»String«ENDFOR»)]) : List[RRow«logicTableName»] = keyset match {   
						   case tail :+  head  =>  getRRow«logicTableName»s2 (ml,tail) :+ RRow«logicTableName»(ml(head))
						   case List() => List.empty[RRow«logicTableName»] 
						      
						 }
						  
						  def convertSetToList (xs : Set[(«FOR gbc:((logic.rowCreationApproachForCube.rowCreationApproach) as FilterAndGroupToOneRowCreationApproach).groupByVariables SEPARATOR ','»String«ENDFOR»)]) : List[(«FOR gbc:((logic.rowCreationApproachForCube.rowCreationApproach) as FilterAndGroupToOneRowCreationApproach).groupByVariables SEPARATOR ','»String«ENDFOR»)] = { 
						    
						     val ys:  List[(«FOR gbc:((logic.rowCreationApproachForCube.rowCreationApproach) as FilterAndGroupToOneRowCreationApproach).groupByVariables SEPARATOR ','»String«ENDFOR»)] = (for(x <- xs) yield  x)(collection.breakOut)
						     return ys
						  }
						  
						  def  getRRow«logicTableName»Maps( rl: List[RRow«sourceTableName»]) : Map[(«FOR gbc:((logic.rowCreationApproachForCube.rowCreationApproach) as FilterAndGroupToOneRowCreationApproach).groupByVariables SEPARATOR ','»String«ENDFOR»),List[RRow«sourceTableName»]]  = rl match {
						    case record =>  rl.groupBy(record => («FOR gbc:((logic.rowCreationApproachForCube.rowCreationApproach) as FilterAndGroupToOneRowCreationApproach).groupByVariables SEPARATOR ','»«sourceTableName»Def.get«gbc.variable_id.toLowerCase»(record)«ENDFOR» ) )
						      
						  } 
						  
						    def getFiltered«sourceTableName»( rl: List[RRow«sourceTableName»]) : List[RRow«sourceTableName»] = rl match {						  		       
						  		       case  tail :+ «sourceTableName» => if («(logic.rowCreationApproachForCube.rowCreationApproach as FilterAndGroupToOneRowCreationApproach).filterFunction.getScalaFunctionString») {getFiltered«sourceTableName»(tail) :+   «sourceTableName» } else getFiltered«sourceTableName»(tail)
						  		       case List() => List.empty[RRow«sourceTableName»]     
						  		  }
				
				 //now for each column get the logic
				 		  «FOR a:logic.columnFunctionGroup.columnFunctions»
				 		  «IF a instanceof StandardBasicColumnFunction»
				 		 def get«a.variable.variable_id.toLowerCase»(r : RRow«logicTableName»)«IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF a.variable.domain_id.isIs_enumerated » :String   «ENDIF»  = r match {  //set the type based on dim, id, measure
				 		 case RRow«logicTableName»(«FOR stn:sourceTableNames SEPARATOR ','»«stn»«ENDFOR») => «a.getScalaFunctionString()» //get the spec from function, check infix, and repalce tablecolumns appropriatelty with function calls.
				 			  } 
				 		  «ENDIF»
				 		 
				 		   
				 		     «IF a instanceof AggregateColumnFunction»
				 		   		     «IF a.aggregateFunction.functionSpec.name.equals("SUM")»
				 		   		     
				 		   		      def get«a.variable.variable_id.toLowerCase»(r : RRow«logicTableName») «IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF a.variable.domain_id.isIs_enumerated » :String   «ENDIF» = r match {  
				 		   		        case RRow«logicTableName»(list) => get«a.variable.variable_id.toLowerCase»2(list)		       
				 		   		    } 
				 		   		    
				 		   		    def get«a.variable.variable_id.toLowerCase»2(r1: List[RRow«sourceTableName»]) «IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF a.variable.domain_id.isIs_enumerated » :String   «ENDIF» = r1 match {  
				 		   		        case tail :+  head  =>  get«a.variable.variable_id.toLowerCase»2(tail) + «sourceTableName»Def.get«a.getRequiredColumn.variable_id.toLowerCase»(head)  
				 		   		         case List() => 0 
				 		   		    }
				 		   		    «ENDIF»	
				 		   		      «IF a.aggregateFunction.functionSpec.name.equals("First")»
				 		   		      def get«a.variable.variable_id.toLowerCase»(r : RRow«logicTableName») «IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF a.variable.domain_id.isIs_enumerated » :String   «ENDIF» = r match {  
				 		   		        case RRow«logicTableName»(list) => get«a.variable.variable_id.toLowerCase»2(list)		       
				 		   		    } 
				 		   		    
				 		   		    def get«a.variable.variable_id.toLowerCase»2(r1: List[RRow«sourceTableName»]) «IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF a.variable.domain_id.isIs_enumerated » :String   «ENDIF» = r1 match {  
				 		   		        case tail :+  head  => «sourceTableName»Def.get«a.getRequiredColumn.variable_id.toLowerCase»(head)  
				 		   		         case List() =>""
				 		   		    }
				 		   
				 		   			«ENDIF»	
				 		   			«ENDIF»	
				 		   
				 		   
				 		«ENDFOR» 
				 		
				 		def gettracingid(r : RRow«logicTableName») :String  = r match {
				 						  case RRow«logicTableName»(list) => "«logicTableName»(" + gettracingid2(list) + ")"	
				 					}
				 					
				 					def gettracingid2(r1: List[RRow«sourceTableName»]) :String = r1 match {  
				 						 case List() :+  head  =>  «sourceTableName»Def.getconciseid(head)
				 						  case tail :+  head  =>   «sourceTableName»Def.getconciseid(head) + ":" + gettracingid2(tail) 
				 						  case List() =>""
				 					}
				 					
				 					def getconciseid(r : RRow«logicTableName») :String  = r match {
				 									  case RRow«logicTableName»(list) => "«logicTableName»:" +md5HashString("«logicTableName»(" + getconciseid2(list) + ")")
				 								}
				 								
				 					def getconciseid2(r1: List[RRow«sourceTableName»]) :String = r1 match {  
				 								case List() :+  head  =>  «sourceTableName»Def.getconciseid(head)
				 								 case tail :+  head  =>   «sourceTableName»Def.getconciseid(head) + ":" + gettracingid2(tail) 
				 								case List() =>""
				 								}
				 		 				
					«ENDIF»
					
				
					def md5HashString(s: String): String = {
				    import java.security.MessageDigest
				    import java.math.BigInteger
				    val md = MessageDigest.getInstance("MD5")
				    val digest = md.digest(s.getBytes)
				    val bigInt = new BigInteger(1,digest)
				    val hashedString = bigInt.toString(16)
				    hashedString
				}
					
		
	
	}
'''
	/**
	 * get UltimateSourceName associated with a cube
	 */
	def String getUltimateSourceName(CubeTransformationLogic logic) {
		val deprls = AttributeLineageUtil.getTheDependantFunctionalRowLogics(logic)

		if( deprls.size > 0 && (deprls.get(0).rowCreationApproachForCube.cube instanceof  BaseViewCube))
			(deprls.get(0).rowCreationApproachForCube.ultimateCubeSource.cube_name)
		else
			null

	}
   
   /**
   * Returns the set of columns (parameters) required by a BasicFunction.
   * 
   * @param function
   * @return
   */
	def List<VARIABLE> getRequiredColumns(BooleanFunction function) {
		return ScalaRuntimeUtil.getRequiredColumns(function)
	}

/**
   * Returns the text representation of a BasicFunction.
   * 
   * @param calculation
   * @return
   */
	def String getScalaFunctionString(ColumnFunction function) {

		return ScalaRuntimeUtil.getcalcText(( function as StandardBasicColumnFunction).basicFunction);

	}

  /**
   * Returns the text representation of a BasicFunction.
   * 
   * @param calculation
   * @return
   */
	def String getScalaFunctionString(BasicFunction function) {

		return ScalaRuntimeUtil.getcalcText(function);

	}
	
   /**
   * Returns the text representation of a BooleanFunction.
   * @param calculation
   * @return
   */
	def String getScalaFunctionString(BooleanFunction function) {
		return ScalaRuntimeUtil.getRowCreationApproachText(function)

	} 

	/**
    * Get the dependent cube names.
    * 
    */
	def List<String> getGetTheDependantCubeeNames(CubeTransformationLogic logic) {
		val List<String> l = new ArrayList<String>();
		val logicList = AttributeLineageUtil.getTheDependantFunctionalRowLogics(logic);
		logicList.forEach[l.add(it.rowCreationApproachForCube.cube.cube_name)]

		val schemaList = AttributeLineageUtil.getTheDependantBaseRowStructures(logic);
		schemaList.forEach[l.add(it.cube.cube_name)]

		return l
	}

	/**
	 * Get the dependent cubes.
	 * 
	 */
	def List<FreeBirdToolsCube> getGetTheDependantCubes(CubeTransformationLogic logic) {
		val List<FreeBirdToolsCube> l = new ArrayList<FreeBirdToolsCube>();
		val logicList = AttributeLineageUtil.getTheDependantFunctionalRowLogics(logic);
		logicList.forEach[l.add(it.rowCreationApproachForCube.cube)]

		val schemaList = AttributeLineageUtil.getTheDependantBaseRowStructures(logic);
		schemaList.forEach[l.add(it.cube)]

		return l
	}

	/**
	 * Returns the BaseRowStrucures which the cubeTransformationLogic  depends upon.
	 * @param cubeTransformationLogic 
	 * @return
	 */
	def List<BaseRowStructure> getTheDependantEvaluatedCubeSchemas(CubeTransformationLogic logic) {

		return AttributeLineageUtil.getTheDependantBaseRowStructures(logic)
	}

	/**
	 * Returns the column/parameter required by a AggregateColumnFunction.
	 * @param function
	 * @return
	 */
	def VARIABLE getGetRequiredColumn(AggregateColumnFunction function) {
		return ScalaRuntimeUtil.getRequiredColumn(function)
	}

	/**
	 * Always returens true, this method needs to be deleted or deprecated
	 * @param function
	 * @return
	 */
	def boolean usedInSubsetsSchema(VARIABLE column, ExecuteAttributeLineageModel test, String string) {
		true
	}
}
