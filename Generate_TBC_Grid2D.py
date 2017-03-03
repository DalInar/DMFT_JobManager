import math
import numpy

L = 7
IncEndPt = True
MaxPhase = 2*math.pi

theta = numpy.linspace(0, MaxPhase, L, IncEndPt)
thetas = [(x,y) for x in theta for y in theta]
PHASE_0 = [x[0] for x in thetas]
PHASE_1 = [x[1] for x in thetas]

print('"PHASE_0": '+ str(PHASE_0)+",")
print('"PHASE_1": '+str(PHASE_1))  
