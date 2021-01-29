package org.eclipse.efbt.cocason.dsl.cocason.scoping

import org.eclipse.xtext.scoping.Scopes
import org.eclipse.xtext.EcoreUtil2
import org.eclipse.efbt.cocalimo.core.model.test_definition.Given
import org.eclipse.efbt.cocalimo.core.model.test_definition.When
import org.eclipse.efbt.cocalimo.core.model.test_definition.Then
import org.eclipse.efbt.cocalimo.core.model.test_definition.TestDefinition
import org.eclipse.efbt.cocalimo.core.model.test_definition.Clause
import org.eclipse.efbt.cocalimo.core.model.test_definition.Param
import org.eclipse.xtext.scoping.IScope
import org.eclipse.emf.ecore.EObject
import org.eclipse.emf.ecore.EReference


class CoCaSonScopeProvider extends AbstractCoCaSonScopeProvider{
	
	
	
	
	override  IScope getScope(EObject context, EReference reference) {
		
        // We want to define the Scope for the Element's superElement cross-reference
        // val LOGGER = LogManager.getLogger(AortaScopeProvider)
 
 /**
     	if ((context instanceof BaseCellWithEnumeratedValue ) && reference.equals(Base_column_structured_dataPackage.Literals.BASE_CELL_WITH_ENUMERATED_VALUE__VALUE))
     	{
     		val cell = context as BaseCellWithEnumeratedValue
     	
     		//can we llok up the index to get stuff...how was this done n eson?
     		
     		val variable = cell.column
     		
     		val resolvedVariable = EcoreUtil2.resolve(variable,  cell)
     		
     		val domain = (resolvedVariable as VARIABLE).domain_id
     		
     		val resolvedDomain =  EcoreUtil2.resolve(domain,  resolvedVariable)
     		
     		val superScope = super.getScope(context, reference)
     		return new FilteringScope(superScope,
			new Predicate<IEObjectDescription>() {
				
				
				override apply(IEObjectDescription input) {
					
					val o = input.EObjectOrProxy 
					//EcoreUtil2.resolve(o,  null as EObject)
				    if (o instanceof MEMBER)
				    {
				    	val mem = o  as MEMBER
						
						
						val resolvedMember =  EcoreUtil2.resolve(mem, null as ResourceSet)
						
						val domain1 =  (resolvedMember as MEMBER).domain_id
						
						val domainName1 = domain1.name
						
						return domainName1.equals((resolvedDomain as DOMAIN).name)
					}
					else 
						return false
				
				}
				
			});
			
     		 
     		
     	}
    
    
        if ((context.eContainer) instanceof Clause )
         {
	         if(context instanceof Param)
	         {
	         	val  bddclauseElement = (context.eContainer) as Clause
	         	val bddElement = (bddclauseElement.eContainer) as TestDefinition
	         	 val coverageTestSet = bddElement.testContraints
	         	  val coverageTestType = coverageTestSet.template
	         	  
	
	         	//find out index of contianing bddclauseElement
	         	val index = bddclauseElement.params.indexOf(context)
	        
	         if (bddclauseElement instanceof Given) {
	        	
	        	val size = coverageTestSet.givenParams.length
	        	if( index < size)
	        	{
		        	val templateObject = coverageTestSet.givenParams.get(index).param
		        	val templateType = coverageTestType.givenParams.get(index).param
		        	
		              
		             System.err.println("given templateObject = " + templateObject)
		             System.err.println("given templateType = " + templateType)
		        	
		        	 val candidates = EcoreUtil2.getAllContentsOfType(templateObject, templateType.class)
		        	 System.err.println("given candidates = " + candidates)
		        	 return Scopes.scopeFor(candidates)
	        	 }
	        	 
	         }
	        	if (bddclauseElement instanceof When) {
	        	
	        	val size = coverageTestSet.whenParams.length
	        	if( index < size)
	        	{
		        		val templateObject = coverageTestSet.whenParams.get(index).param
		        	val templateType = coverageTestType.whenParams.get(index).param
		        	
		              
		             System.err.println("when templateObject = " + templateObject)
		             System.err.println("when templateType = " + templateType)
		        	
		        	 val candidates = EcoreUtil2.getAllContentsOfType(templateObject, templateType.class)
		        	 
		        	 return Scopes.scopeFor(candidates)
	        	 }
	        	 
	         }
	         
	         if (bddclauseElement instanceof Then) {
	        	
	        	val size = coverageTestSet.thenParams.length
	        	if( index < size)
	        	{
	        		
	        	
	        	val templateObject = coverageTestSet.thenParams.get(index).param
	        	val templateType = coverageTestType.thenParams.get(index).param
	        	
	              
	             System.err.println("then templateObject = " + templateObject)
	             System.err.println("then templateType = " + templateType)
	        	
	        	 val candidates = EcoreUtil2.getAllContentsOfType(templateObject, templateType.class)
	        	 
	        	 return Scopes.scopeFor(candidates)
	        	 
	        	 }
	         }
	            
	        
	    }
	    
	    }
	    * 
	    */
    return super.getScope(context, reference)
    }
}