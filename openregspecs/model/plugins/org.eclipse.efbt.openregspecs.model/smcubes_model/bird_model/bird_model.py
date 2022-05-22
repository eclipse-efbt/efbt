"""Definition of meta model 'bird_model'."""
from functools import partial
import pyecore.ecore as Ecore
from pyecore.ecore import *


name = 'bird_model'
nsURI = 'http://www.eclipse.org/efbt/bird_model'
nsPrefix = 'bird_model'

eClass = EPackage(name=name, nsURI=nsURI, nsPrefix=nsPrefix)

eClassifiers = {}
getEClassifier = partial(Ecore.getEClassifier, searchspace=eClassifiers)


class BIRDModel(EObject, metaclass=MetaEClass):

    entityModules = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    smcubesCoreModel = EReference(ordered=True, unique=True, containment=True, derived=False)

    def __init__(self, *, entityModules=None, smcubesCoreModel=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if entityModules:
            self.entityModules.extend(entityModules)

        if smcubesCoreModel is not None:
            self.smcubesCoreModel = smcubesCoreModel
