package org.eclipse.efbt.bird.component.modelquery;

import java.io.File;

import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;

import org.eclipse.efbt.bird.bird_model.BIRDModel;

/**
 * This class is responsible for  getting the default BIRD model associated 
 * with any object 
 * 
 * @author Neil Mackenzie
 *
 */
public class Util {

	/**
	 * Tet the default BIRD model associated with any object. This relies on 
	 * the  file layout convention being follwoed correcty.
	 *  
	 * @param o
	 * @return
	 */
	public static BIRDModel getDefaultBirdModel(EObject o) {
	
		  ResourceSet rs = o.eResource().getResourceSet();
		    String birdModelXMLFile = o.eResource().getURI().trimSegments(1)
		        + "/bird_model.bird_model";
		    File file = new File(birdModelXMLFile);
		    URI uri = file.isFile() ? URI.createFileURI(file.getAbsolutePath()) : URI.createURI(birdModelXMLFile);
		    Resource resource = rs.getResource(uri, true);
		    return (BIRDModel) resource.getContents().get(0);
	}

}
