def processReportModule(ReportModule reportModule, IFileSystemAccess2 fsa) {
		fsa.generateFile("report_cells.xcore",  '''
		package report_cells
		
		import output_tables.*
		import sdd_domains.* 
		
		«FOR report : reportModule.reports»
		«IF report instanceof CellBasedReport»
		«FOR cell : (report as CellBasedReport).reportCells»
		class Cell_«report.outputLayer.name»_«cell.row.name»_«cell.column.name»
		{
			refers «report.outputLayer.name»_Table «report.outputLayer.name.giveSmallFirstLetter»_Table
			refers «report.outputLayer.name»[] «report.outputLayer.name.giveSmallFirstLetter»s
			
			op String metric_value() {
				«report.outputLayer.name.giveSmallFirstLetter»s.fold(0, [  a, b |  a + b.«cell.metric.name»() ]).toString				
				}
			op  void calc_referenced_items() {
						
					val items = «report.outputLayer.name.giveSmallFirstLetter»_Table.«report.outputLayer.name.giveSmallFirstLetter»s.filter(item | 
					«FOR filter : cell.filters SEPARATOR ' && '»
					«IF filter.member.size > 0»(«FOR literal : filter.member SEPARATOR ' || '»(item.«filter.operation.name».value == (sdd_domains.«(literal.eContainer() as ELEnum).name».«literal.name.toUpperCase».value ))«ENDFOR»)«ELSE» true
					«ENDIF»
					«ENDFOR»	
					)			
						for( «report.outputLayer.name» item : items )
								{
									
										«report.outputLayer.name.giveSmallFirstLetter»s.add(item)
								}				
				}
			op String  init() {
					org.eclipse.efbt.regpot_desktop.orchestrator.Orchestration.init(this)
				 	calc_referenced_items() 
				return null
					}
		}
		«ENDFOR»
		«ENDIF»
		«ENDFOR»
		
		''')
	}
	
	def createOutputTablesAmendment(RulesForReport rulesForReport, IFileSystemAccess2 fsa) {
		fsa.generateFile(rulesForReport.outputLayerCube.name + "_output_layer_amendment.xcore",  '''
		package «rulesForReport.outputLayerCube.name»_output_layer_amendment
		import sdd_domains.*
		import input_tables.*
		import output_tables.*
		import «rulesForReport.outputLayerCube.name»_Logic.*
		class «rulesForReport.outputLayerCube.name» {
			refers «rulesForReport.outputLayerCube.name»_UnionItem  unionOfLayers 
		«FOR eloperation : rulesForReport.outputLayerCube.EOperations»
		
		«IF eloperation instanceof ELOperation» 	op «eloperation.EType.name» «IF eloperation.upperBound == -1»[]  «ELSEIF !((eloperation.lowerBound == 0) && ( (eloperation.upperBound == 1) || (eloperation.upperBound == 0)) ) »[«eloperation.lowerBound»..«eloperation.upperBound»]«ENDIF» «eloperation.name»()
			{
				unionOfLayers.«eloperation.name»()
			}
		
		«ENDIF»«ENDFOR» 
		}
		class «rulesForReport.outputLayerCube.name»_Table {
						refers  «rulesForReport.outputLayerCube.name»_UnionTable  unionOfLayersTable
						contains  «rulesForReport.outputLayerCube.name» [] «rulesForReport.outputLayerCube.name»s 
						op  «rulesForReport.outputLayerCube.name» [] calc_«rulesForReport.outputLayerCube.name»s() {
							var items = new org.eclipse.emf.common.util.BasicEList<«rulesForReport.outputLayerCube.name»>()
						for( «rulesForReport.outputLayerCube.name»_UnionItem item : unionOfLayersTable.«rulesForReport.outputLayerCube.name»_UnionItems)
						{
							var newItem = Output_tablesFactory.eINSTANCE.create«rulesForReport.outputLayerCube.name»
							newItem.unionOfLayers =  item
							items.add(newItem)
		}
						return items
							} 
						op String  init() {
							org.eclipse.efbt.regpot_desktop.orchestrator.Orchestration.init(this)
						 this.«rulesForReport.outputLayerCube.name»s.addAll(calc_«rulesForReport.outputLayerCube.name»s()) 
		 				return null
							}
					} 

		''')
	}
	
	def processRulesForReport(RulesForReport rulesForReport, IFileSystemAccess2 fsa) {
		fsa.generateFile(rulesForReport.outputLayerCube.name + "_Logic.xcore",  '''
		package «rulesForReport.outputLayerCube.name»_Logic
		import sdd_domains.*
		import input_tables.*
		import output_tables.*
		annotation "dep" as dep
		class «rulesForReport.outputLayerCube.name»_UnionItem {
			refers «rulesForReport.outputLayerCube.name»_Base base 
		«FOR eloperation : rulesForReport.outputLayerCube.EOperations»
		
		«IF eloperation instanceof ELOperation»		@dep (dep1="base.«eloperation.name»")
		 	op «eloperation.EType.name» «IF eloperation.upperBound == -1»[]  «ELSEIF !((eloperation.lowerBound == 0) && ( (eloperation.upperBound == 1) || (eloperation.upperBound == 0)) ) »[«eloperation.lowerBound»..«eloperation.upperBound»]«ENDIF» «eloperation.name»()
		 	{
				base.«eloperation.name»()
			}
			
		
		«ENDIF»«ENDFOR» 
		}
		class «rulesForReport.outputLayerCube.name»_Base {
			
		«FOR eloperation : rulesForReport.outputLayerCube.EOperations»
				
		«IF eloperation instanceof ELOperation» 	op «eloperation.EType.name» «IF eloperation.upperBound == -1»[]  «ELSEIF !((eloperation.lowerBound == 0) && ( (eloperation.upperBound == 1) || (eloperation.upperBound == 0)) ) »[«eloperation.lowerBound»..«eloperation.upperBound»]«ENDIF»  «eloperation.name»()
			{
			«IF eloperation.EType.name == "double" »return 0
			«ELSEIF eloperation.EType.name == "int" »return 0
			«ELSEIF eloperation.EType.name == "boolean" »return true
			«ENDIF»
		}
		
		«ENDIF»«ENDFOR» 
		}
		
		class «rulesForReport.outputLayerCube.name»_UnionTable {
					 	contains  «rulesForReport.outputLayerCube.name»_UnionItem []   «rulesForReport.outputLayerCube.name»_UnionItems	
					 	«FOR tableRules : rulesForReport.rulesForTable»
					 	«FOR tablePartRules : tableRules.rulesForTablePart»
					 	refers «tablePartRules.name»_Table  «tablePartRules.name»_Table
					 	«ENDFOR»
					 	«ENDFOR»
					 	op «rulesForReport.outputLayerCube.name»_UnionItem []   calc_«rulesForReport.outputLayerCube.name»_UnionItems() 
						{
						 	var items = new org.eclipse.emf.common.util.BasicEList<«rulesForReport.outputLayerCube.name»_UnionItem>()
						 	«FOR tableRules : rulesForReport.rulesForTable»
						 	«FOR tablePartRules : tableRules.rulesForTablePart»
		 		 		 	for(«tablePartRules.name» item : «tablePartRules.name.giveSmallFirstLetter»_Table.«tablePartRules.name.giveSmallFirstLetter»s)
		 		 		 	{
		 		 		 		var newItem = «rulesForReport.outputLayerCube.name»_LogicFactory.eINSTANCE.create«rulesForReport.outputLayerCube.name»_UnionItem
		 		 		 		newItem.base = item
		 		 		 		items.add(newItem)
		 		 		 	}
						 	«ENDFOR»
						 	«ENDFOR»									
						 	return items
						}
					 	op String  init() 
						{
						 	org.eclipse.efbt.regpot_desktop.orchestrator.Orchestration.init(this) 
			 	 			this.«rulesForReport.outputLayerCube.name.giveSmallFirstLetter»_UnionItems.addAll(calc_«rulesForReport.outputLayerCube.name»_UnionItems)
			 	 			  return null
						}
		
					}
		
		«FOR tableRules : rulesForReport.rulesForTable»
		«FOR tablePartRules : tableRules.rulesForTablePart»
		class «tablePartRules.name»  extends «rulesForReport.outputLayerCube.name»_Base {

		«FOR column : tablePartRules.columns»
				«IF (column instanceof SelectColumnAttributeAs)»
				  // remove any duplicates in these refers statements
				  refers «(column.attribute.eContainer as ELClass).name» «(column.attribute.eContainer as ELClass).name.giveSmallFirstLetter»
				«ENDIF»	
		«ENDFOR»
		«FOR column : tablePartRules.columns»	
		«IF ( column instanceof SelectDerivedColumnAs) »
		@dep (dep1="«(column.attribute.eContainer as ELClass).name».«column.attribute.name»")
		«ENDIF»
		«IF (column instanceof SelectColumnAttributeAs)  »
		@dep (dep1="«(column.attribute.eContainer as ELClass).name».«column.attribute.name»")
		«ENDIF»			
		op «column.asAttribute.EType.name»  «column.asAttribute.name»() 
		{
			«IF (column instanceof SelectColumnMemberAs)»
			sdd_domains.«column.asAttribute.EType.name».«column.memberAsConstant.name.toUpperCase»
			«ELSEIF (column instanceof SelectValueAs)»
			"«column.value»"
			«ELSEIF (column instanceof SelectColumnAttributeAs)»
			«(column.attribute.eContainer as ELClass).name».«column.attribute.name»
			«ELSEIF ( column instanceof SelectDerivedColumnAs) »
			«(column.attribute.eContainer as ELClass).name».«column.attribute.name»
			«ENDIF»	
		}
		«ENDFOR»
		}
		
		class «tablePartRules.name»_Table {
						«FOR column : tablePartRules.columns»
						«IF (column instanceof SelectColumnAttributeAs)»
						refers «(column.attribute.eContainer as ELClass).name»_Table «(column.attribute.eContainer as ELClass).name.giveSmallFirstLetter»_Table
						// remove any duplicates in these refers statements
						«ENDIF»	
						«ENDFOR»
					 	contains  «tablePartRules.name» []   «tablePartRules.name»s	
					 	op «tablePartRules.name» []   calc_«tablePartRules.name»s() 
						{
						 	var items = new org.eclipse.emf.common.util.BasicEList<«tablePartRules.name»>()
				 		 	//Join up any refered tables that you need to join
				 		 	//loop through the main table
				 		 	//create an item using var newItem = «rulesForReport.outputLayerCube.name»_LogicFactory.eINSTANCE.create«tablePartRules.name»
				 		 	//set any references you want to on the new Item so that it can refer to themin operations
				 		 	return items
						}
					 	op String  init() 
						{
						 	org.eclipse.efbt.regpot_desktop.orchestrator.Orchestration.init(this)
				 		 	this.«tablePartRules.name.giveSmallFirstLetter»s.addAll(calc_«tablePartRules.name»s) 		 
				 		 	return null
						}
					}
		«ENDFOR»
		«ENDFOR»

		        ''')
	}
	
	def giveSmallFirstLetter(String string) {
		var firstLetter = string.substring(0,1)
		var smallFirstLetter = firstLetter.toLowerCase()
		return smallFirstLetter + string.substring(1,string.length)
		
	}
	
	def createXCoreForPackage(ELPackage elpackage, IFileSystemAccess2 fsa,Resource resource) {
		fsa.generateFile(elpackage.name + '.xcore',  '''
		
		package «elpackage.name»
		«IF elpackage.name.trim != "types"»
		«FOR theImport : elpackage.imports»
		
		«IF theImport.importedNamespace.trim != "types.*"»
		import «theImport.importedNamespace» 
		«ENDIF»
		«ENDFOR»
		«FOR annotationDirective : elpackage.annotationDirectives»
				annotation "«annotationDirective.sourceURI»" as «annotationDirective.name»
		«ENDFOR»
		«FOR elclass : elpackage.EClassifiers.filter(ELClass)»
		«FOR annotion : elclass.EAnnotations»
				@«annotion.source.name» («FOR detail : annotion.details SEPARATOR ","» «detail.key»="«detail.value»"«ENDFOR»)
		«ENDFOR»
		«IF elclass.EAbstract»abstract «ENDIF»class «elclass.name» «IF elclass.ESuperTypes.length == 1» extends «elclass.ESuperTypes.get(0).name» «ENDIF»{
		
		
		«FOR elmember : elclass.EStructuralFeatures»  
		«FOR annotion : elmember.EAnnotations»
			«IF true»	@«annotion.source.name» («FOR detail : annotion.details SEPARATOR ","» «detail.key»="«detail.value»"«ENDFOR»)«ENDIF»
		«ENDFOR»
		«IF elmember instanceof ELAttribute» 	«IF elmember.ID»id «ENDIF»«elmember.EAttributeType.name» «IF elmember.upperBound == -1»[]  «ELSEIF !((elmember.lowerBound == 0) && ( (elmember.upperBound == 1) || (elmember.upperBound == 0)) ) »[«elmember.lowerBound»..«elmember.upperBound»]«ENDIF» «elmember.name» «ENDIF»
		«IF elmember instanceof ELReference» 	«IF elmember.containment»contains «ELSE»refers«ENDIF» «elmember.EType.name» «IF elmember.upperBound == -1»[]  «ELSEIF !((elmember.lowerBound == 0) && ( (elmember.upperBound == 1) || (elmember.upperBound == 0)) ) »[«elmember.lowerBound»..«elmember.upperBound»]«ENDIF» «elmember.name» «IF elmember.EOpposite !== null »	opposite «elmember.EOpposite.name»«ENDIF»«ENDIF»
		
		«ENDFOR»
		«FOR eloperation : elclass.EOperations»
		«FOR annotion : eloperation.EAnnotations»
			«IF true»	@«annotion.source.name» («FOR detail : annotion.details SEPARATOR ","» «detail.key»="«detail.value»"«ENDFOR»)«ENDIF»
		«ENDFOR»
		«IF eloperation instanceof ELOperation» 	op «eloperation.EType.name» «IF eloperation.upperBound == -1»[]  «ELSEIF !((eloperation.lowerBound == 0) && ( (eloperation.upperBound == 1) || (eloperation.upperBound == 0)) ) »[«eloperation.lowerBound»..«eloperation.upperBound»]«ENDIF» «eloperation.name»()
			{
		«IF eloperation.body !== null »          «findXCoreSubstring(eloperation.body)»
		«ELSEIF eloperation.EType.name == "double" »        return 0
		«ELSEIF eloperation.EType.name == "int" »        return 0
		«ELSEIF eloperation.EType.name == "boolean" »        return true
			«ENDIF»
			}
			«ENDIF»«ENDFOR» 
		}
		«IF elpackage.name.trim != "ldm_entities"»
		class «elclass.name»_Table {
			contains  «elclass.name» [] «elclass.name»s 								
		} 
		«ENDIF»
		«ENDFOR»
		«FOR elEnum : elpackage.EClassifiers.filter(ELEnum)»
		enum «elEnum.name» {«FOR elliteral : elEnum.ELiterals»  «elliteral.name»  as "«elliteral.literal»"  = «elliteral.value» «ENDFOR»}
		«ENDFOR»
		«FOR xDataType : resource.allContents.filter(ELDataType).toIterable»
		«IF !(xDataType instanceof ELEnum)»
		type  «xDataType.name» wraps «IF xDataType.name == "Date"»java.util.Date «ELSE»«xDataType.name» «ENDIF» 
		«ENDIF»	
		
		«ENDFOR»
		«ENDIF»
		        ''')
		         }
	