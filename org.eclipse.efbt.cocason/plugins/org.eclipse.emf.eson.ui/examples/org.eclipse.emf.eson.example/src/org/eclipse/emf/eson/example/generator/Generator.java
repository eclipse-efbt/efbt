package org.eclipse.emf.eson.example.generator;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.eson.example.library.Library;
import org.eclipse.emf.eson.generators.GeneratorHelper;
import org.eclipse.xtext.generator.IFileSystemAccess;
import org.eclipse.xtext.generator.IGenerator;

import com.google.common.base.Optional;

// IGenerator in the RUNTIME project (NOT Plugin), right next to the ESONs, loaded dynamically
// NOTE: This class is registered in META-INF/services/org.eclipse.xtext.generator.IGenerator
public class Generator implements IGenerator {

	// NOTE: cannot currently use @Inject here
	GeneratorHelper h = new GeneratorHelper();
	Template t = new Template();
	
	@Override public void doGenerate(Resource input, IFileSystemAccess fsa) {
	    // Example using dynamic EMF API
		EObject eo = input.getContents().get(1); // 1 instead of 0 for ESON
		Object name = eo.eGet(eo.eClass().getEStructuralFeature("name"));
		fsa.generateFile(input.getURI().lastSegment() + ".dyn-api.txt", "hello " + name);
		
        // Example using static EMF API gen. from XCore
		Optional<Library> opt = h.getFirstRootContentOfType(input, Library.class);
		if (!opt.isPresent())
			return;
		Library library = opt.get();
		
		CharSequence text = t.generate(library);
		fsa.generateFile(input.getURI().lastSegment() + ".txt", text);
	}
	
}
