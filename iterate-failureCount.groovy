// Implemented in ExecuteGroovyScript Processor
def flowFile = session.get()
if(!flowFile) return
 	
if (flowFile.getAttribute('failureCount')) {	
    flowFile = session.putAttribute(flowFile, 'failureCount', (flowFile.getAttribute('failureCount').toInteger() + 1).toString())	
} else {	
    flowFile = session.putAttribute(flowFile, 'failureCount', '1')
}
session.transfer(flowFile,REL_SUCCESS)
