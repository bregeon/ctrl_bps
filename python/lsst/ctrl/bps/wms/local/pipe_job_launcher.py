from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.Interfaces.API.Job import Job
from DIRAC.Interfaces.API.Dirac import Dirac

# argument should be the absolute path to a wrapper
args = Script.getPositionalArgs()
print(args)

pipetask_wrapper = args[0]
inputs = args[1]
input_files = inputs.split(' ')
input_files.append(pipetask_wrapper)

print(input_files)
# prepare and submit simple job
dirac = Dirac()
j = Job()
j.setCPUTime(500)
j.setExecutable(pipetask_wrapper)
j.setName('lsst pipe test')
j.setInputSandbox(input_files)
# j.setDestination('LCG.IN2P3-CC.fr')
res = dirac.submitJob(j, mode='local')
print('Submission Result: ',res['Value'])
