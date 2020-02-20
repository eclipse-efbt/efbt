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
 package org.eclipse.efbt.generator.scala.templates


import org.eclipse.emf.ecore.resource.Resource
import org.eclipse.xtext.generator.IFileSystemAccess
import org.eclipse.xtext.generator.IGenerator




import org.eclipse.emf.common.util.EList
import row_transformation_logic.FunctionalRowLogic
import row_transformation_logic.BaseRowStructure
import base_cube_data.BaseCubeData
import column_transformation_logic.ColumnFunction
import org.eclipse.efbt.model.util.ScalaRuntimeUtil
import column_transformation_logic.StandardBasicColumnFunction
import base_cube_data.BaseCell
import cube_transformation_logic.RowJoinFunction
import cube_transformation_logic.OneToOneRowFunction
import java.util.List
import java.util.ArrayList
import functions.BooleanFunction
import cube_transformation_logic.RowFilterFunction

import functions.BasicFunction
import cube_transformation_logic.RowGroupByFunction
import column_transformation_logic.AggregateColumnFunction
import cube_transformation_logic.FilterAndGroupToOneRowFunction
import functions.ResolvedCubeColumnParameter
import functions.AggregateFunction


import trl_sql_views.SQLView

import core.VARIABLE
import base_cube_data.BaseCellWithEnumeratedValue
import base_cube_data.BaseCellWithValue
import core.FACET_VALUE_TYPE




import cubes.FreeBirdToolsCube
import cubes.BaseCube

import platform_call.ExecuteAttributeLineageModel

import attribute_lineage.AttributeLineageModel
import cube_transformation_logic.UnionRowFunction
import org.eclipse.efbt.model.util.AttributeLineageUtil
/**
 * Generateor tmplate for generating scala files
 */
class JavaBeanTemplate implements IGenerator {

	/**
	 * Generate
	 */
	override doGenerate(Resource input, IFileSystemAccess fsa) {

		
		input.allContents.filter(ExecuteAttributeLineageModel).forEach[
		
			
			
			val content2 = generateTheCellModelCreator(it)
		    fsa.generateFile("TheCellModelCreator.scala", content2)
			val sourceCubes = it.programInputs.sourceTableData
			sourceCubes.forEach[
				
				val content = generateScalaCodeForTableData(it)
				val cubename = it.cube.cube_name
				
				fsa.generateFile(cubename +"Data.scala", content)
				
			]
			val rowLogicGroup = it.attributeLineageModel
			rowLogicGroup.rowTransformations.forEach[
			
			
					val cubename = it.cubeLogic.cube.cube_name
					
					val content = generateScalaCodeForRowLogic(it,it.cubeLogic.cube.cube_name,it.getTheDependantCubeeNames, it.getTheDependantCubes)
					
				
					
					fsa.generateFile(cubename +".scala", content)
			

			]
		
		rowLogicGroup.baseSchemas.forEach[
			
				
					val content = generateSourceSchemaScalaCode(it)
					val cubename = it.cube.cube_name
					System.out.println("SourceSchema" + cubename)
					fsa.generateFile(cubename +".scala", content)
			
				
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
		«FOR schema:test.attributeLineageModel.baseSchemas»
				import «schema.cube.cube_name»_def._  
				import «schema.cube.cube_name»_data._  
		«ENDFOR»
		«FOR logic:test.attributeLineageModel.rowTransformations»
		import «logic.cubeLogic.cube.cube_name»_def._  
		«ENDFOR»
		
		object TheCellModelCreator extends App {
		  override def  main(args: Array[String]) {
		«FOR schema:test.attributeLineageModel.baseSchemas»
		 val t_«schema.cube.cube_name»: RTable«schema.cube.cube_name»  = RTable«schema.cube.cube_name»(«schema.cube.cube_name»Data.initialiseRowList()) 		 
		«ENDFOR»
		«val orderedRowTransformation = test.attributeLineageModel.orderedRowTransformations»
		«FOR logic:orderedRowTransformation»
		 val t_«logic.cubeLogic.cube.cube_name»: RTable«logic.cubeLogic.cube.cube_name»= RTable«logic.cubeLogic.cube.cube_name»(«FOR sourceTableName:logic.getTheDependantCubeeNames SEPARATOR ','»t_«sourceTableName»«ENDFOR»)				
		«ENDFOR»
		
		def stringForHeader = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + "<data_lineage:DataLineageModel xmlns:data_lineage=\"http://www.eclipse.org/data_lineage\" xmlns:base_cube_data=\"http://www.eclipse.org/base_cube_data\" xmlns:functions=\"http://www.eclipse.org/functions\" >\n" 
						  
				val stringForFile = stringForHeader + 
				«FOR logic:test.attributeLineageModel.rowTransformations SEPARATOR "+"»stringFor«logic.cubeLogic.cube.cube_name»Table(t_«logic.cubeLogic.cube.cube_name») «ENDFOR»+ «FOR schema:test.attributeLineageModel.baseSchemas SEPARATOR "+"»stringFor«schema.cube.cube_name»Table(t_«schema.cube.cube_name») «ENDFOR» +stringForEndOfXML()
				
					import java.io._
				    val pw = new PrintWriter(new File("«outputdir»\\generated.data_lineage" ))
				    pw.write(stringForFile)
				    pw.close
		}
		«FOR sourceTableSchema:test.programInputs.sourceTableData»
		
			«val theSchemaName = sourceTableSchema.cube.cube_name»
							def  stringFor«theSchemaName»Table(table_«theSchemaName»: RTable«theSchemaName» ) :String = 
									{				 
									   "<sourceData name=\"«theSchemaName»\" cube=\"«sourceTableSchema.cube.eResource.URI.toString»#«sourceTableSchema.cube.cube_name»\"> \n" +
									   «FOR row:sourceTableSchema.rows»
									    "<rows rowID=\"«row.rowID»\">\n" +
									   	«FOR cell: row.cells»
									   	«val column= cell.column»
									   	«IF column.usedInSubsetsSchema(test,theSchemaName)»"<cells xsi:type=\"base_cube_data:«IF cell instanceof BaseCellWithEnumeratedValue»BaseCellWithEnumeratedValue«ENDIF»«IF cell instanceof BaseCellWithValue»BaseCellWithValue«ENDIF»\" cellID=\"«cell.cellID»\" column=\"«column.eResource.URI.toString»#«column.variable_id»\" value=\"«IF cell instanceof BaseCellWithEnumeratedValue»«cell.value.eResource.URI.toString»#«cell.value.member_id»«ENDIF»«IF cell instanceof BaseCellWithValue»«cell.value»«ENDIF»\"/>\n" +					   						     					   						     
									   		«ENDIF»								   
									   	 «ENDFOR»
									   "</rows>\n" +
									   «ENDFOR»
									   "</sourceData>\n "
								     
									}
	«ENDFOR»
						
							
					        			
	
		«FOR logic:test.attributeLineageModel.rowTransformations»
		
		
		«val logicName = logic.cubeLogic.cube.cube_name»
		
		«val sourceTableNames =logic.getTheDependantCubeeNames»
		def  stringFor«logicName»Table(table_«logicName»: RTable«logicName» ) :String = 
		{
		   
		  
		   "<dataTraceableByCell name=\"«logicName»\" cubeLogic=\"«logic.cubeLogic.eResource.URI.toString»#«logic.cubeLogic.name»\" sourceCubeData=\"«FOR stn:sourceTableNames SEPARATOR ' '»#«stn»«ENDFOR»\" cube=\"«logic.cubeLogic.cube.eResource.URI.toString»#«logic.cubeLogic.cube.cube_name»\"> \n" +
		   stringFor«logicName»Table2(«logicName»Def.getRRows«logicName»(table_«logicName»)) +   
		   "</dataTraceableByCell>\n "
		     
		}
		
		
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
		  
		   «IF (!(logic.cubeLogic.rowFunction instanceof RowGroupByFunction) && !(logic.cubeLogic.rowFunction instanceof FilterAndGroupToOneRowFunction) )»
		   «FOR stn:sourceTableNames»
		   		  			val sourcerow«stn» = row.«stn»
		   		  			val sourcerowID«stn» =«stn»Def.getconciseid(sourcerow«stn»)
		   		  «ENDFOR»	
		   		  		    "<rows rowID=\"" +rowID + "\" sourceRows=\"" +  «FOR stn:sourceTableNames SEPARATOR "+\" \" +"»"#" + sourcerowID«stn»«ENDFOR»+"\">\n" +
		   		  		stringForRow«logicName»(row)  + 
		   		  		"</rows>\n" 
		   
			  
		 «ELSE»
		 
				    "<rows rowID=\"" +rowID + "\" sourceRows=\"" + getListOfIdsforSourceRowsOf«logicName»(row) + "\">\n" +		  		  
		  		stringForGroupRow«logicName»(row)  + 
		  		"</rows>\n" 
		  
		  «ENDIF»
		  
		}
		
		 «IF ((logic.cubeLogic.rowFunction instanceof RowGroupByFunction) || (logic.cubeLogic.rowFunction instanceof FilterAndGroupToOneRowFunction))»
				  
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
		 «IF ((logic.cubeLogic.rowFunction instanceof RowGroupByFunction) || (logic.cubeLogic.rowFunction instanceof FilterAndGroupToOneRowFunction))»
		def getParamsForTheSourceRowsOf«logicName»(«logicName»Row :RRow«logicName», col :String) :String =  {
		
		   getParamsForSourceRowsOf«logicName»(«logicName»Row.«sourceTableNames.get(0)», col)
		}
		 «ENDIF»
		 
		 
		 
		def getParamsForSourceRowsOf«logicName»(«sourceTableNames.get(0)»Rows :List[RRow«sourceTableNames.get(0)»], col :String) :String = «sourceTableNames.get(0)»Rows match {
		  case  tail :+ head => getParamsForSourceRowsOf«logicName»(tail,col) +   getParamForSourceRowOf«logicName»(head,col) 
		       case List() => "" 
		}
		
		def getParamForSourceRowOf«logicName»(«sourceTableNames.get(0)»Row :RRow«sourceTableNames.get(0)», col :String) :String = {
		  
		  val rowId = «sourceTableNames.get(0)»Def.getconciseid(«sourceTableNames.get(0)»Row)
		  "     <parameters xsi:type=\"functions:CellsParameter\" cells=\"#" + rowId +":" +col+ "\"/>\n"
		}
		
		
		
		def stringForGroupRow«logicName»(row : RRow«logicName») :String     =  { 
					
							 
					val rowID = «logicName»Def.getconciseid(row)
					 
					 
				  «FOR column:logic.columnFunctionGroup.columnFunctions»
				  
				  val «column.variable.variable_id.toLowerCase» = «logicName»Def.get«column.variable.variable_id.toLowerCase»(row)
				  
				
				 val «column.variable.variable_id.toLowerCase»Text =	«IF column.usedInSubsets == column.usedInSubsets»"<derivedCells cellID=\"" + rowID + ":«column.variable.variable_id»\" column=\"«column.variable.eResource.URI.toString»#«column.variable.variable_id»\" value=\"" + «column.variable.variable_id.toLowerCase» + "\">\n" +
				    «IF column instanceof AggregateColumnFunction»
				    «val aggregateFunction = column.aggregateFunction»
				    «aggregateFunction.getXMLForAggregateFunctionParameterisedBySourceRowID(logicName)»
				     «ENDIF»
	      "   </derivedCells>\n"
	      «ENDIF» ««««IF !column.usedInSubsets»""«ENDIF»
				  «ENDFOR»
				
				   «FOR column:logic.columnFunctionGroup.columnFunctions SEPARATOR "+"»«column.variable.variable_id.toLowerCase»Text«ENDFOR»
			   
				} 
		
		«IF (true)» 
		def stringForRow«logicName»(row : RRow«logicName») :String     =  { 
			
					 
			val rowID = «logicName»Def.getconciseid(row)
			 
			 «FOR stn:sourceTableNames»
			 «IF (!(logic.cubeLogic.rowFunction instanceof RowGroupByFunction) && !(logic.cubeLogic.rowFunction instanceof FilterAndGroupToOneRowFunction))»
			val sourcerow«stn» = row.«stn»
			val sourcerowID«stn» =«stn»Def.getconciseid(sourcerow«stn»)
			«ENDIF»
			  «ENDFOR»
		  «FOR column:logic.columnFunctionGroup.columnFunctions»
		  val «column.variable.variable_id.toLowerCase» = «logicName»Def.get«column.variable.variable_id.toLowerCase»(row)
		 
		
		 val «column.variable.variable_id.toLowerCase»Text =	«IF column.usedInSubsets == column.usedInSubsets»"<derivedCells cellID=\"" + rowID + ":«column.variable.variable_id»\" column=\"«column.variable.eResource.URI.toString»#«column.variable.variable_id»\" value=\"" + «column.variable.variable_id.toLowerCase» + "\">\n" +
		    «IF (!(logic.cubeLogic.rowFunction instanceof RowGroupByFunction) && !(logic.cubeLogic.rowFunction instanceof FilterAndGroupToOneRowFunction))»
		    «IF column instanceof StandardBasicColumnFunction»
		    «val basicfunction = column.basicFunction»
		    «basicfunction.getXMLForBasicFunctionParameterisedBySourceRowID()»
		     «ENDIF»
		     «ENDIF»
		     "   </derivedCells>\n"
		     «ENDIF»««««IF !column.usedInSubsets»""«ENDIF»
		  «ENDFOR»
		
		   «FOR column:logic.columnFunctionGroup.columnFunctions SEPARATOR "+"»«column.variable.variable_id.toLowerCase»Text«ENDFOR»
	   
		} 
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
	 * Get the user FunctionalRowLogic objects form the AttributeLineageUtil in the 
	 * correct order so that dependencies always come before the FunctionalRowLogics 
	 * objects that they are dependent upon.
	 */
	def EList<FunctionalRowLogic> getOrderedRowTransformations(AttributeLineageModel program)
	{
		val orderedList =  AttributeLineageUtil.getOrderedRowTransformations(program)			
		return orderedList;
				
	}
	
	
	
	
	
	/**
	 * get the XML For AggregateFunctions Parameterised By SourceRowID
	 */
	def String getXMLForAggregateFunctionParameterisedBySourceRowID(AggregateFunction function, String logicName)
	{
		  val List<String> l = new ArrayList<String>();
		 l.add("\"   <function xsi:type=\\\"functions:AggregateFunction\\\" functionSpec=\\\"" + function.functionSpec.eResource.URI.toString+ "#"+ function.functionSpec.name + "\\\">\\n\" + " )
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
		 val funcName = function.functionSpec.name 
		 l.add("\"   <function xsi:type=\\\"functions:BasicFunction\\\" functionSpec=\\\"" + function.functionSpec.eResource.URI.toString+ "#"+ function.functionSpec.name + "\\\">\\n\" + \n" )
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
	
	
	«IF (false)»
	
	«ELSE»
	import base_types._ 
	import «data.cube.cube_name»_def._
	
	object  «data.cube.cube_name»Data {
		
	
	def initialiseRowList ()  :List[RRow«data.cube.cube_name»]  = {
	
	  val rowlist : List[RRow«data.cube.cube_name»] = List.empty[RRow«data.cube.cube_name»] 
	  «FOR a:data.rows» 
	     val «a.rowID» : RRow«data.cube.cube_name» = RRow«data.cube.cube_name»( "«a.rowID»" , «FOR b:a.cells SEPARATOR ','» «IF b instanceof BaseCellWithEnumeratedValue»"«b.value.name»"«ENDIF»«IF b instanceof BaseCellWithValue && (b.column.domain_id.data_type == FACET_VALUE_TYPE.STRING)»"«b.value»"«ENDIF»«IF b instanceof BaseCellWithValue && (b.column.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)»«b.value»«ENDIF»«ENDFOR»)
	
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
	
	«IF (true)»
	
	case class RTable«schema.cube.cube_name»(rl : List[RRow«schema.cube.cube_name»])  extends RTable 
	case class RRow«schema.cube.cube_name» ( conciseid : String, «FOR c:schema.columns SEPARATOR ','»«c.variable.variable_id.toLowerCase» « IF (!c.variable.domain_id.isIs_enumerated && c.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!c.variable.domain_id.isIs_enumerated && c.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF c.variable.domain_id.isIs_enumerated » :String   «ENDIF» «ENDFOR») extends RRow	
		
	object  «schema.cube.cube_name»Def {	
	def getRRows«schema.cube.cube_name»( t: RTable«schema.cube.cube_name») : List[RRow«schema.cube.cube_name»] = t match {
				      case RTable«schema.cube.cube_name»(rl) => rl
				  
				  }
		  «FOR a:schema.columns»
		 
		   
		  def get«a.variable.variable_id.toLowerCase»(r : RRow«schema.cube.cube_name»)«IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!a.variable.domain_id.isIs_enumerated && a.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF a.variable.domain_id.isIs_enumerated » :String   «ENDIF» = r match {
		      case RRow«schema.cube.cube_name»(conciseid : String, «FOR b:schema.columns SEPARATOR ','»«b.variable.variable_id.toLowerCase» «IF (!b.variable.domain_id.isIs_enumerated && b.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!b.variable.domain_id.isIs_enumerated && b.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF b.variable.domain_id.isIs_enumerated » :String   «ENDIF»  «ENDFOR» ) => «a.variable.variable_id.toLowerCase»
		  }
				  		
		«ENDFOR» 
		
		  def getconciseid(r : RRow«schema.cube.cube_name») :String  = r match { 
		 		case RRow«schema.cube.cube_name»( conciseid : String, «FOR b:schema.columns SEPARATOR ','»«b.variable.variable_id.toLowerCase» «IF (!b.variable.domain_id.isIs_enumerated && b.variable.domain_id.data_type == FACET_VALUE_TYPE.STRING)» :String «ENDIF»« IF (!b.variable.domain_id.isIs_enumerated && b.variable.domain_id.data_type == FACET_VALUE_TYPE.DOUBLE)» :Double «ENDIF»« IF b.variable.domain_id.isIs_enumerated » :String   «ENDIF»  «ENDFOR» ) =>  conciseid : String
		 		  }	
		 
	
	}
	«ENDIF»
	
	
'''
	/**
	 * generate Scala Code For RowLogic
	 */
	def generateScalaCodeForRowLogic(FunctionalRowLogic logic,String logicTableName,List<String> sourceTableNames, List<FreeBirdToolsCube> sourceCubes) '''
	package «logic.cubeLogic.cube.cube_name»_def
	import base_types._ 
	import funcdefs._ 
	import customfuncs._
	«val sourceTableName = sourceTableNames.get(0)»
	«val sourceCube = sourceCubes.get(0)»

		
			
	«IF logic.cubeLogic.rowFunction instanceof OneToOneRowFunction»

	
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
	«IF logic.cubeLogic.rowFunction instanceof UnionRowFunction»
	
		
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
	
	«IF logic.cubeLogic.rowFunction instanceof RowJoinFunction»
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
	     if («(logic.cubeLogic.rowFunction as RowJoinFunction).joinFunction.getScalaFunctionString»)
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
	
	«IF logic.cubeLogic.rowFunction instanceof RowGroupByFunction»
		«val groupbycol1 = ((logic.cubeLogic.rowFunction) as RowGroupByFunction).groupByColumns.get(0).variable_id.toLowerCase»
		
		
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
		    
		
		    
		  def getRRow«logicTableName»s2 (ml: Map[(«FOR gbc:((logic.cubeLogic.rowFunction) as RowGroupByFunction).groupByColumns SEPARATOR ','»String«ENDFOR»),List[RRow«sourceTableName»]] , keyset : List [(«FOR gbc:((logic.cubeLogic.rowFunction) as RowGroupByFunction).groupByColumns SEPARATOR ','»String«ENDFOR»)]) : List[RRow«logicTableName»] = keyset match {   
		   case tail :+  head  =>  getRRow«logicTableName»s2 (ml,tail) :+ RRow«logicTableName»(ml(head))
		   case List() => List.empty[RRow«logicTableName»] 
		      
		 }
		  
		  def convertSetToList (xs : Set[(«FOR gbc:((logic.cubeLogic.rowFunction) as RowGroupByFunction).groupByColumns SEPARATOR ','»String«ENDFOR»)]) : List[(«FOR gbc:((logic.cubeLogic.rowFunction) as RowGroupByFunction).groupByColumns SEPARATOR ','»String«ENDFOR»)] = { 
		    
		     val ys:  List[(«FOR gbc:((logic.cubeLogic.rowFunction) as RowGroupByFunction).groupByColumns SEPARATOR ','»String«ENDFOR»)] = (for(x <- xs) yield  x)(collection.breakOut)
		     return ys
		  }
		  
		  def  getRRow«logicTableName»Maps( rl: List[RRow«sourceTableName»]) : Map[(«FOR gbc:((logic.cubeLogic.rowFunction) as RowGroupByFunction).groupByColumns SEPARATOR ','»String«ENDFOR»),List[RRow«sourceTableName»]]  = rl match {
		    case record =>  rl.groupBy(record => («FOR gbc:((logic.cubeLogic.rowFunction) as RowGroupByFunction).groupByColumns SEPARATOR ','»«sourceTableName»Def.get«gbc.variable_id.toLowerCase»(record)«ENDFOR» ) )
		      
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
	«IF logic.cubeLogic.rowFunction instanceof RowFilterFunction»
		import «sourceTableName»_def._  //imprt each source table for this table
		
		
		case class RTable«logicTableName»(t1: RTable«sourceTableName») extends RTable 
		case class RRow«logicTableName»(«sourceTableName»: RRow«sourceTableName») 
		
		object «logicTableName»Def  {
		   
		  def getRRows«logicTableName»(t : RTable«logicTableName») : List[RRow«logicTableName»]= t match {
		      case RTable«logicTableName»(someTable) => getRRows«logicTableName»(«sourceTableName»Def.getRRows«sourceTableName»(someTable))
		  }
		  
		  def getRRows«logicTableName»( rl: List[RRow«sourceTableName»]) : List[RRow«logicTableName»] = rl match {
		       case  tail :+ «sourceTableName» => if («(logic.cubeLogic.rowFunction as RowFilterFunction).filterFunction.getScalaFunctionString») {getRRows«logicTableName»(tail) :+   RRow«logicTableName»(«sourceTableName») } else getRRows«logicTableName»(tail)
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
		
			«IF logic.cubeLogic.rowFunction instanceof FilterAndGroupToOneRowFunction»
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
						    
						
						    
						  def getRRow«logicTableName»s2 (ml: Map[(«FOR gbc:((logic.cubeLogic.rowFunction) as FilterAndGroupToOneRowFunction).groupByVariables SEPARATOR ','»String«ENDFOR»),List[RRow«sourceTableName»]] , keyset : List [(«FOR gbc:((logic.cubeLogic.rowFunction) as FilterAndGroupToOneRowFunction).groupByVariables SEPARATOR ','»String«ENDFOR»)]) : List[RRow«logicTableName»] = keyset match {   
						   case tail :+  head  =>  getRRow«logicTableName»s2 (ml,tail) :+ RRow«logicTableName»(ml(head))
						   case List() => List.empty[RRow«logicTableName»] 
						      
						 }
						  
						  def convertSetToList (xs : Set[(«FOR gbc:((logic.cubeLogic.rowFunction) as FilterAndGroupToOneRowFunction).groupByVariables SEPARATOR ','»String«ENDFOR»)]) : List[(«FOR gbc:((logic.cubeLogic.rowFunction) as FilterAndGroupToOneRowFunction).groupByVariables SEPARATOR ','»String«ENDFOR»)] = { 
						    
						     val ys:  List[(«FOR gbc:((logic.cubeLogic.rowFunction) as FilterAndGroupToOneRowFunction).groupByVariables SEPARATOR ','»String«ENDFOR»)] = (for(x <- xs) yield  x)(collection.breakOut)
						     return ys
						  }
						  
						  def  getRRow«logicTableName»Maps( rl: List[RRow«sourceTableName»]) : Map[(«FOR gbc:((logic.cubeLogic.rowFunction) as FilterAndGroupToOneRowFunction).groupByVariables SEPARATOR ','»String«ENDFOR»),List[RRow«sourceTableName»]]  = rl match {
						    case record =>  rl.groupBy(record => («FOR gbc:((logic.cubeLogic.rowFunction) as FilterAndGroupToOneRowFunction).groupByVariables SEPARATOR ','»«sourceTableName»Def.get«gbc.variable_id.toLowerCase»(record)«ENDFOR» ) )
						      
						  } 
						  
						    def getFiltered«sourceTableName»( rl: List[RRow«sourceTableName»]) : List[RRow«sourceTableName»] = rl match {						  		       
						  		       case  tail :+ «sourceTableName» => if («(logic.cubeLogic.rowFunction as FilterAndGroupToOneRowFunction).filterFunction.getScalaFunctionString») {getFiltered«sourceTableName»(tail) :+   «sourceTableName» } else getFiltered«sourceTableName»(tail)
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
	def String getUltimateSourceName(FunctionalRowLogic logic) {
		val deprls = AttributeLineageUtil.getTheDependantFunctionalRowLogics(logic)

		if (false)
			(deprls.get(0).cubeLogic.ultimateCubeSource.cube_name)
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
		return ScalaRuntimeUtil.getRowFunctionText(function)

	}

	/**
    * Get the dependent cube names.
    * 
    */
	def List<String> getGetTheDependantCubeeNames(FunctionalRowLogic logic) {
		val List<String> l = new ArrayList<String>();
		val logicList = AttributeLineageUtil.getTheDependantFunctionalRowLogics(logic);
		logicList.forEach[l.add(it.cubeLogic.cube.cube_name)]

		val schemaList = AttributeLineageUtil.getTheDependantBaseRowStructures(logic);
		schemaList.forEach[l.add(it.cube.cube_name)]

		return l
	}

	/**
	 * Get the dependent cubes.
	 * 
	 */
	def List<FreeBirdToolsCube> getGetTheDependantCubes(FunctionalRowLogic logic) {
		val List<FreeBirdToolsCube> l = new ArrayList<FreeBirdToolsCube>();
		val logicList = AttributeLineageUtil.getTheDependantFunctionalRowLogics(logic);
		logicList.forEach[l.add(it.cubeLogic.cube)]

		val schemaList = AttributeLineageUtil.getTheDependantBaseRowStructures(logic);
		schemaList.forEach[l.add(it.cube)]

		return l
	}

	/**
	 * Returns the BaseRowStrucures which the functionalRowLogic depends upon.
	 * @param functionalRowLogic
	 * @return
	 */
	def List<BaseRowStructure> getTheDependantEvaluatedCubeSchemas(FunctionalRowLogic logic) {

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
