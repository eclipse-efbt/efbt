
package org.eclipse.efbt.clan.component.exporter;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;


import org.eclipse.efbt.clan.model.clan.ClanPackage;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.xtext.resource.XtextResourceSet;

import com.google.inject.Injector;

/**
 * XMLToXTextConverter is responsible for taking an ecore model instance
 * representing a 'Program', which has been serialised to XML format, and
 * creating an XText representation of this according to the grammar defined in
 * the clan XText grammar.
 * 
 * Note that the clan XText grammar is generated from the Ecore model
 * representing 'Program' s.
 * 
 * @author Neil Mackenzie
 *
 *
 */
public class XMLToXTextConverter {

	/**
	 * Convert from xml files on the outpath to new xtext files on the output path.
	 * For no we limit this to domains and input data structures
	 * 
	 * @param inputpath
	 * @param outputPath
	 * @throws IOException
	 */
	public void convertXMLToXtext(String inputpath, String outputPath) throws IOException {
		// load the set of files into a resource set.
		// persist the files as a set of xtext files

		/**EPackage.Registry.INSTANCE.put("http://www.eclipse.org/clan_program", clan_programPackage.eINSTANCE);
		FreeBirdToolsResourceFactory factory = new FreeBirdToolsResourceFactory();

		Resource.Factory.Registry reg = Resource.Factory.Registry.INSTANCE;
		Map<String, Object> m = reg.getExtensionToFactoryMap();
		m.put("clan_program", factory);

		clanStandaloneSetup setup = new clanStandaloneSetup();
		Injector i = setup.createInjectorAndDoEMFRegistration();
		
		//load the xml files into a resource, using the FreeBirdToolsResourceFactory
		ResourceSet resSet = i.getInstance(ResourceSet.class);

		
		URI domainsProgramURI = URI.createFileURI(
				inputpath + "domains.clan_program");
		URI inputDataStructuresProgramURI = URI.createFileURI(
				inputpath + "inputDataStructures.clan_program");

		Resource domainsResource = resSet.
				createResource(domainsProgramURI);
		Resource inputDataStructuresResource = resSet.
				createResource(inputDataStructuresProgramURI);

		domainsResource.load(Collections.EMPTY_MAP);
		inputDataStructuresResource.load(Collections.EMPTY_MAP);

		//make sure we resolve all proxies or we have problems.
		EcoreUtil.resolveAll(resSet);

		EObject domainsRoot = domainsResource.getContents().get(0);
		EObject inputDataStructuresRoot = 
				inputDataStructuresResource.getContents().get(0);

		// now create an xtext resource from the resource
		XtextResourceSet xrs = i.getInstance(XtextResourceSet.class);

		URI trgtURI = URI.createFileURI("C:/Users/Neil/output3/domains.dsl");

		URI trgtURI2 = URI.
				createFileURI("C:/Users/Neil/output3/inputDataStructures.dsl");

		Resource domainsResource2 = xrs.createResource(trgtURI);
		Resource inputDataStructuresResource2 = xrs.createResource(trgtURI2);

		domainsResource2.getContents().add(domainsRoot);

		inputDataStructuresResource2.getContents().
						add(inputDataStructuresRoot);

		EcoreUtil.resolveAll(xrs);
		
		//save the xtext representation of the model
		domainsResource2.save(Collections.EMPTY_MAP);
		inputDataStructuresResource2.save(Collections.EMPTY_MAP);*/

	}

	/**
	 * main method to retrieve the input and output file paths as argumanets
	 * and call the conversion.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		XMLToXTextConverter converter = new XMLToXTextConverter();
		try {
			
			converter.convertXMLToXtext(args[0], args[1]);
		} catch (IOException e) {
		
			e.printStackTrace();
		}

	}

}
