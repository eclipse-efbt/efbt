package org.eclipse.emf.eson.example.library.generator

import org.eclipse.emf.eson.example.library.Library

class Template {
    
    def generate(Library library) '''
        EXAMPLE created by IGenerator in plugin

        «library.name»
        ====

        «FOR book : library.books»
        * «book.name»
        «ENDFOR»
    '''
    
}