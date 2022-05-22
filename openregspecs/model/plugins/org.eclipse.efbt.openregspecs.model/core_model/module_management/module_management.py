"""Definition of meta model 'module_management'."""
from functools import partial
import pyecore.ecore as Ecore
from pyecore.ecore import *


name = 'module_management'
nsURI = 'http://www.eclipse.org/efbt/module_management'
nsPrefix = 'module_management'

eClass = EPackage(name=name, nsURI=nsURI, nsPrefix=nsPrefix)

eClassifiers = {}
getEClassifier = partial(Ecore.getEClassifier, searchspace=eClassifiers)


class Module(EObject, metaclass=MetaEClass):
    """A grouping of related artifacts, persisted as a resource, with a version number, and a description, and a description of which Modules it depends upon (and importantly which version of those Modules)."""
    theDescription = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    license = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    version = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    dependencies = EReference(ordered=True, unique=True, containment=True, derived=False)
    longName = EReference(ordered=True, unique=True, containment=True, derived=False)

    def __init__(self, *, dependencies=None, theDescription=None, license=None, name=None, version=None, longName=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if theDescription is not None:
            self.theDescription = theDescription

        if license is not None:
            self.license = license

        if name is not None:
            self.name = name

        if version is not None:
            self.version = version

        if dependencies is not None:
            self.dependencies = dependencies

        if longName is not None:
            self.longName = longName


class ModuleLongName(EObject, metaclass=MetaEClass):
    """A long name associated with a module"""
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)

    def __init__(self, *, name=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if name is not None:
            self.name = name


class ModuleDependencies(EObject, metaclass=MetaEClass):
    """a  list of module dependencies"""
    theModules = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, theModules=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if theModules:
            self.theModules.extend(theModules)


class ModuleDependency(EObject, metaclass=MetaEClass):
    """A module dependency, including its version number and its long name"""
    moduleName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    moduleVersion = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    theModule = EReference(ordered=True, unique=True, containment=False, derived=False)
    longName = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, moduleName=None, moduleVersion=None, theModule=None, longName=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if moduleName is not None:
            self.moduleName = moduleName

        if moduleVersion is not None:
            self.moduleVersion = moduleVersion

        if theModule is not None:
            self.theModule = theModule

        if longName is not None:
            self.longName = longName
