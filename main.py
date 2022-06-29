import db
from pathlib import Path
import procesa_tipos




cuota = input('ingrese la cuota :')
home = Path.cwd()
ruta = Path(home,'cuotas',cuota)
files = ruta.glob("*.dat")

for file in files:

        if "FALLECIDOS" in str(file):
           procesa_tipos.procesa_fallecidos(file,cuota)
        else:
           procesa_tipos.procesa_tipo_1(file,cuota)
           procesa_tipos.procesa_tipo_2(file,cuota)
           procesa_tipos.procesa_tipo_3(file,cuota)


        
        


