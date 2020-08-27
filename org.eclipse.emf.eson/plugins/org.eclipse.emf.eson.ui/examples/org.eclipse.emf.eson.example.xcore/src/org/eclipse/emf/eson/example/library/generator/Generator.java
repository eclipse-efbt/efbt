package org.eclipse.emf.eson.example.library.generator;

import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.eson.example.library.Library;
import org.eclipse.emf.eson.generators.GeneratorHelper;
import org.eclipse.xtext.generator.IFileSystemAccess;
import org.eclipse.xtext.generator.IGenerator;

import com.google.common.base.Optional;

// IGenerator in the PLUGIN project, right next to the Xcore (Ecore)
// NOTE: This class is registered via standard Eclipse extension point in ./plugin.xml
public class Generator implements IGenerator {

	// NOTE: cannot currently use @Inject here
    private GeneratorHelper h = new GeneratorHelper();
    private Template t = new Template();
	
	@Override public void doGenerate(Resource input, IFileSystemAccess fsa) {
		Optional<Library> opt = h.getFirstRootContentOfType(input, Library.class);
		if (!opt.isPresent())
			return;
		Library library = opt.get();
		
		CharSequence text = t.generate(library);
		fsa.generateFile(input.getURI().lastSegment() + "-by_plugin.txt", text);
	}

}
