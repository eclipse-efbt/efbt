
from .module_management import getEClassifier, eClassifiers
from .module_management import name, nsURI, nsPrefix, eClass
from .module_management import Module, ModuleLongName, ModuleDependencies, ModuleDependency


from . import module_management

__all__ = ['Module', 'ModuleLongName', 'ModuleDependencies', 'ModuleDependency']

eSubpackages = []
eSuperPackage = None
module_management.eSubpackages = eSubpackages
module_management.eSuperPackage = eSuperPackage

Module.dependencies.eType = ModuleDependencies
Module.longName.eType = ModuleLongName
ModuleDependencies.theModules.eType = ModuleDependency
ModuleDependency.theModule.eType = Module
ModuleDependency.longName.eType = ModuleLongName

otherClassifiers = []

for classif in otherClassifiers:
    eClassifiers[classif.name] = classif
    classif.ePackage = eClass

for classif in eClassifiers.values():
    eClass.eClassifiers.append(classif.eClass)

for subpack in eSubpackages:
    eClass.eSubpackages.append(subpack.eClass)
