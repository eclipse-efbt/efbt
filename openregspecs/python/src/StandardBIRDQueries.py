from pyecore.resources import ResourceSet, URI


        
        
class StandardBIRDQueries(object):
    
   
    
    def query1(self,fileDirectory): 
        rset = ResourceSet()
        resource = rset.get_resource(URI(fileDirectory + "\\bird.ecore"))
        mm_root = resource.contents[0]
        rset.metamodel_registry[mm_root.nsURI] = mm_root
        print(mm_root.eClassifiers[0])
        
        
if __name__ == '__main__':
    fileDirectory = 'C:\\Users\\LENOVO\\freebirdtools-develop-dec22\\git\\efbt\\openregspecs\\python\\resources'
    standardBIRDQueries = StandardBIRDQueries()
    standardBIRDQueries.query1(fileDirectory)
    