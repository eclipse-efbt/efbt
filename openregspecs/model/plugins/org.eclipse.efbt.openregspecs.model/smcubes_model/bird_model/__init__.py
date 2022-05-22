
from .bird_model import getEClassifier, eClassifiers
from .bird_model import name, nsURI, nsPrefix, eClass
from .bird_model import BIRDModel

from data_meta_model import EntityModule
from openregspecs_smcubes_core_extension import SMCubesCoreModel

from . import bird_model

__all__ = ['BIRDModel']

eSubpackages = []
eSuperPackage = None
bird_model.eSubpackages = eSubpackages
bird_model.eSuperPackage = eSuperPackage

BIRDModel.entityModules.eType = EntityModule
BIRDModel.smcubesCoreModel.eType = SMCubesCoreModel

otherClassifiers = []

for classif in otherClassifiers:
    eClassifiers[classif.name] = classif
    classif.ePackage = eClass

for classif in eClassifiers.values():
    eClass.eClassifiers.append(classif.eClass)

for subpack in eSubpackages:
    eClass.eSubpackages.append(subpack.eClass)
