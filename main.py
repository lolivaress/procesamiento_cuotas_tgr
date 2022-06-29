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
           #procesa_tipos.procesa_tipo_2(file,cuota)
           #procesa_tipos.procesa_tipo_3(file,cuota)



cnn = db.SQLAlchemyConnection('localhost','dgio_procesa_tgr_2022')
cnn.connect()
tabla = f"cuota_{cuota}_T2"
query_string = f"exec dbo.sp_normaliza_sw {tabla}"
cnn.execute(query_string)
cnn.close()

        
        


