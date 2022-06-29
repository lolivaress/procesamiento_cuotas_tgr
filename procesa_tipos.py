
from io import StringIO
import os
from sqlalchemy.types import BigInteger, Integer, SmallInteger, String
import pandas as pd
import db

#string connection
cnn = db.SQLAlchemyConnection('localhost','dgio_procesa_tgr_2022')

def ultima_linea(archivo):
    with open(archivo, "rb") as file:
        file.seek(-2, os.SEEK_END)
        while file.read(1) != b'\n':
            file.seek(-2, os.SEEK_CUR)
        last_line = file.readline().decode()
    return last_line


def procesa_tipo_1(file , cuota):
    largo = [1, 4, 8, 8, 10, 1, 9, 3, 15]
    columnas = ['tipo_registro', 'numero_proceso_tgr', 'fecha_proceso_tgr', 'fecha_de_pago',
                'rut_mutual_o_isl', 'dv_mutual_o_isl', 'banco', 'tipo_cuenta', 'numero_cuenta']
    tipos=  {'tipo_registro': SmallInteger(),
                'numero_proceso_tgr': BigInteger(), 
                'fecha_proceso_tgr': String(10),
                'fecha_de_pago': String(10),
                'rut_mutual_o_isl': String(10),
                'dv_mutual_o_isl': String(1),
                'banco': String(10),
                'tipo_cuenta': String(3),
                'numero_cuenta': String(20) ,
                'cuota': String(5),
                'archivo': String(100)
                }

    
    df_cabeza = pd.read_fwf(file, delimiter=None, index_col=False, index=False,
                            dtype=str, header=None,  encoding="cp1252", nrows=1, widths=largo, names=columnas)

    df_cabeza['cuota']=cuota
    df_cabeza['archivo']=file.name

    cnn.connect()
    df_cabeza.to_sql('cuota_'+cuota+"_T1", cnn.engine, if_exists='replace', index=False,
                     dtype=tipos)
    cnn.close()



def procesa_tipo_2(file,cuota):
    largo = [1, 10, 1, 50, 50, 50, 50, 50, 50, 6, 15, 1, 6, 0, 5, 15, 15, 15, 15, 15, 15, 2, 15, 15, 15, 15, 15, 15, 15,
             1, 15, 1, 15, 4, 1, 16, 1, 12]
    columnas = ['tipo_registro',   'rut_trabajador', 'dv_trabajador', 'nombres',  'apellido_paterno',
                'apellido_materno',   'direccion postal',  'comuna',   'correo_electronico',   'actividad_economica',
                'identificador_de_calculo', 'cond_mutual', 'fecha_primera_informacion_de_renta',       'condicion_mutual',
                'tasa_cotizacion_aplicada_mutual',    'monto_cotizacion_del_seguro_aplicado_por_sii_que_se_debio_enterar',
                'monto_cotizacion_del_seguro_a_enterar_con_cargo_a_las_retenciones',   'saldo_neto_por_cotizar_seguro',
                'monto_ley_sanna_calculado_SII_que_debio_enterar',  'monto_ley_sanna_enteradas_con_retenciones',
                'saldo_neto_por_cotiza_ley_sanna',  'numero_de_cuota_a_pagar',    'monto_cuota_seguro_sin_reajuste',
                'monto_cuota_neto_a_pagar_seguro_mutual_reajustado',   'saldo_en_cuotas_seguro_sin_reajuste',
                'monto_cuota_ley_sanna_sin_reajuste',   'monto_cuota_a_pagar_ley_sanna_reajustado',
                'saldo_en_cuotas_ley_sanna_sin_reajuste',     'monto_cuota_depositado_por_TGR',
                'diferencia_entre_saldo_pendiente_a_pago_a_mutual_seguro_proceso_anterior_y_proceso_actual',
                'variable_1',   'diferencia_entre_saldo_pendiente_de_pago_sanna_proceso_anterior_y_actual',
                'variable_2',   'anio_tributario',   'se_acoge_a_rebaja',  'fecha_hora_calculo_sii',
                'tipo_registro_informado',    'folio_egreso']

    tipo = {'tipo_registro': String(1),   'rut_trabajador': Integer(), 'dv_trabajador':String(1),
            'nombres': String(50),  'apellido_paterno': String(50),
            'apellido_materno': String(50),   'direccion postal': String(50),
            'comuna': String(50),   'correo_electronico': String(50),   'actividad_economica': String(6),
            'identificador_de_calculo': String(15),
            'cond_mutual': String(1),
            'fecha_primera_informacion_de_renta': String(6),
            'condicion_mutual': String(1),
            'tasa_cotizacion_aplicada_mutual': String(5),    'monto_cotizacion_del_seguro_aplicado_por_sii_que_se_debio_enterar': BigInteger(),
            'monto_cotizacion_del_seguro_a_enterar_con_cargo_a_las_retenciones': BigInteger(),   'saldo_neto_por_cotizar_seguro': BigInteger(),
            'monto_ley_sanna_calculado_SII_que_debio_enterar': BigInteger(),  'monto_ley_sanna_enteradas_con_retenciones': BigInteger(),
            'saldo_neto_por_cotiza_ley_sanna': BigInteger(),  'numero_de_cuota_a_pagar': String(2),    'monto_cuota_seguro_sin_reajuste': BigInteger(),
            'monto_cuota_neto_a_pagar_seguro_mutual_reajustado': BigInteger(),   'saldo_en_cuotas_seguro_sin_reajuste': BigInteger(),
            'monto_cuota_ley_sanna_sin_reajuste': BigInteger(),   'monto_cuota_a_pagar_ley_sanna_reajustado': BigInteger(),
            'saldo_en_cuotas_ley_sanna_sin_reajuste': BigInteger(),     'monto_cuota_depositado_por_TGR': BigInteger(),
            'variable_1':String(15),
            'diferencia_entre_saldo_pendiente_de_pago_sanna_proceso_anterior_y_actual':String(1),
            'diferencia_entre_saldo_pendiente_a_pago_a_mutual_seguro_proceso_anterior_y_proceso_actual': String(1),
            'variable_2': String(15),   'anio_tributario': String(4),   'se_acoge_a_rebaja': String(1),
            'fecha_hora_calculo_sii': String(16),  'tipo_registro_informado': String(1),
            'folio_egreso': String(12),  'cuota': String(3), 'nombre_archivo': String(100)}


    df_tipo_2 = pd.read_fwf(file, delimiter=None, index_col=False, index=False, widths=largo, names=columnas, dtype=str,
                     header=None, skipfooter=1, skiprows=1, encoding="ISO-8859-1")
    df_tipo_2['cuota']=cuota
    df_tipo_2['nombre_archivo']=file.name
    cnn.connect()
    df_tipo_2.to_sql('cuota_'+cuota+'_T2',cnn.engine, if_exists='replace', index=False, dtype=tipo)
    cnn.close()



def procesa_tipo_3(file,cuota):

    largo = [1, 10, 15]
    columnas = ['tipo_de_registro',
                'cantidad_registros_tipo_2', 'monto_depositado']

    tipos=  {'tipo_registro': SmallInteger(),
            'cantidad_registros_tipo_2': Integer(),
            'monto_depositado': BigInteger(),
            'cuota': String(5),
            'archivo': String(100)
    }

    last_line =str(ultima_linea(str(file)))
    
    df_tipo_3 = pd.read_fwf(StringIO(last_line), header=None,dtype=str,  encoding="ISO-8859-1", widths=largo, names=columnas,na_filter=False,
     index_col=False,)
    df_tipo_3['cuota']=cuota
    df_tipo_3['archivo']=file.name

    cnn.connect()
    df_tipo_3.to_sql('cuota_'+cuota+'_T3',cnn.engine, if_exists='replace', index=False, dtype=tipos)
    cnn.close()



def procesa_fallecidos(file,cuota):

    df_fallecidos = pd.read_csv(file, sep=";", encoding="ISO-8859-1",dtype=str)
    df_fallecidos['cuota']=cuota
    df_fallecidos['archivo']=file.name
    tipo={'Rut Trabajador': String(10),
          'Dv Trabajador': String(1),
          'Nombres' : String(80),
          'Monto Saldo Seguro': BigInteger(),
          'Monto Saldo Sanna': BigInteger(),
          'Fecha Defuncion': String(10),
          'Cuota': String(5),
          'archivo': String(100)
          }

    cnn.connect()
    df_fallecidos.to_sql('Fallecidos_cuota_'+cuota, cnn.engine, if_exists='replace', index=False,dtype=tipo)
    #cnn.execute(f"delete from fallecidos_consolidado where cuota='{cuota}'")
    #df_fallecidos.to_sql('Fallecidos_consolidado', cnn.engine, if_exists='append', index=False,dtype=tipo)
    cnn.close()
