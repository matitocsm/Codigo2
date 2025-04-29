# Lo anterior para crear un ejecutable del py
# pip install pyinstaller
# pyinstaller --onefile --console procesador_contable.py
# Hola mundo!

import os
import re
import time
import calendar
import pandas as pd
from openpyxl import load_workbook
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from openpyxl.utils.dataframe import dataframe_to_rows

# Mapa de meses en español a número
SPANISH_MONTHS = {
    'enero':'01','febrero':'02','marzo':'03','abril':'04',
    'mayo':'05','junio':'06','julio':'07','agosto':'08',
    'septiembre':'09','octubre':'10','noviembre':'11','diciembre':'12'
}

def parse_fecha(fecha_str: str) -> datetime:
    m = re.search(r'([A-Za-zñÑáéíóúÁÉÍÓÚ]+)\s+(\d{4})', fecha_str)
    if not m:
        raise ValueError(f"No pude parsear la fecha: {fecha_str!r}")
    mes_sp, año = m.groups()
    mes_num = SPANISH_MONTHS.get(mes_sp.lower())
    if not mes_num:
        raise ValueError(f"Mes desconocido: {mes_sp!r}")
    año, mes = int(año), int(mes_num)
    _, ultimo_dia = calendar.monthrange(año, mes)
    return datetime(año, mes, ultimo_dia).date()

def process_file(path: str) -> pd.DataFrame:
    # 1) Intentamos leer hasta 10 veces si PermissionError
    for i in range(10):
        try:
            df_raw = pd.read_excel(path, header=None, dtype=str,
                                   keep_default_na=False, engine='openpyxl')
            break
        except PermissionError:
            time.sleep(1)
    else:
        raise PermissionError(f"Acceso denegado tras 10 intentos: {path}")

    # 2) Extraer fecha
    fecha = parse_fecha(df_raw.iat[4, 0])

    # 3) Localizar encabezado
    is_hdr = df_raw.apply(
        lambda r: r.astype(str).str.lower()
                     .str.contains('código cuenta contable').any(),
        axis=1
    )
    if not is_hdr.any():
        raise ValueError("No encontré 'Código cuenta contable'")
    hr = is_hdr.idxmax()

    # 4) Reconstruir tabla
    df_body = df_raw.iloc[hr:].reset_index(drop=True)
    df_body.columns = df_body.iloc[0]
    df_data = df_body.iloc[1:].reset_index(drop=True)

    # 5) Columnas de código, nombre y transaccional
    code_col = next(c for c in df_data.columns
                    if 'código cuenta contable' in c.lower())
    name_col = next(c for c in df_data.columns
                    if 'nombre' in c.lower() and 'cuenta' in c.lower())
    trans_col = next(c for c in df_data.columns
                     if 'transaccional' in c.lower())

    # 6) Métricas numéricas
    metrics = ['Saldo inicial','Movimiento débito','Movimiento crédito','Saldo final']
    for m in metrics:
        if m in df_data.columns:
            df_data[m] = pd.to_numeric(df_data[m], errors='coerce').fillna(0)

    # 7) Lookup de nombres
    name_map = dict(zip(df_data[code_col], df_data[name_col]))

    # 8) Filtrar sólo filas transaccionales = "Sí"
    df_trans = df_data[
        df_data[trans_col].str.strip().str.lower() == 'sí'
    ].copy()

    # 9) Desglose jerárquico
    df_trans['Clase']     = df_trans[code_col].str[:1]
    df_trans['Grupo']     = df_trans[code_col].str[:2]
    df_trans['Cuenta']    = df_trans[code_col].str[:4]
    df_trans['Subcuenta'] = df_trans[code_col].str[:6]

    # 10) Auxiliar: si el código tiene al menos 8 dígitos, tomo los 8 primeros; si no, "no aplica"
    df_trans['Auxiliar'] = df_trans[code_col].apply(
        lambda x: x[:8] if len(x) >= 8 else 'no aplica'
    )
    df_trans['Nombre_auxiliar'] = df_trans['Auxiliar'].map(name_map).fillna('no aplica')

    # 11) Nombres niveles
    df_trans['Nombre Clase']  = df_trans['Clase'].map(name_map).fillna('no aplica')
    df_trans['Nombre_Grupo']  = df_trans['Grupo'].map(name_map).fillna('no aplica')
    df_trans['Nombre_cuenta'] = df_trans['Cuenta'].map(name_map).fillna('no aplica')
    df_trans['Nombre_sub']    = df_trans['Subcuenta'].map(name_map).fillna('no aplica')

    # 12) Columnas originales y fecha, con empty‐string y NaN → "no aplica"
    if 'Sucursal' in df_trans.columns:
        df_trans['Sucursal'] = df_trans['Sucursal'] \
                                   .replace('', 'no aplica') \
                                   .fillna('no aplica')
    else:
        df_trans['Sucursal'] = 'no aplica'

    if 'Nombre tercero' in df_trans.columns:
        df_trans['Nombre tercero'] = df_trans['Nombre tercero'] \
                                         .replace('', 'no aplica') \
                                         .fillna('no aplica')
    else:
        df_trans['Nombre tercero'] = 'no aplica'

    df_trans['Fecha'] = fecha

    # 13) Columnas finales
    final_cols = [
      'Clase','Nombre Clase',
      'Grupo','Nombre_Grupo',
      'Cuenta','Nombre_cuenta',
      'Subcuenta','Nombre_sub',
      'Auxiliar','Nombre_auxiliar',
      'Sucursal','Nombre tercero',
      'Saldo inicial','Movimiento débito',
      'Movimiento crédito','Saldo final',
      'Fecha'
    ]
    df_out = df_trans.reindex(columns=final_cols)

    # 14) Rellenar con "no aplica" todos los campos vacíos
    return df_out.fillna('no aplica')

class ExcelHandler(FileSystemEventHandler):
    def __init__(self, watch_dir: str, output_dir: str):
        self.watch_dir  = watch_dir
        self.output_dir = output_dir
        self.final_path = os.path.join(output_dir, "procesado_final.xlsx")

    def on_created(self, event):
        if not event.src_path.lower().endswith(".xlsx"):
            return
        try:
            df_new = process_file(event.src_path)

            # Si ya existe, chequeo fecha duplicada
            if os.path.exists(self.final_path):
                df_old = pd.read_excel(self.final_path, engine='openpyxl')
                fechas_old = set(df_old['Fecha'].astype(str).unique())
                fecha_new  = str(df_new['Fecha'].iat[0])
                if fecha_new in fechas_old:
                    print(f"[SKIP] {os.path.basename(event.src_path)} ya procesado.")
                    return

                # Abrir y anexar sólo nuevas filas
                wb = load_workbook(self.final_path)
                ws = wb.active
                for r in dataframe_to_rows(df_new, index=False, header=False):
                    ws.append(r)
                wb.save(self.final_path)
            else:
                df_new.to_excel(self.final_path, index=False)

            print(f"Añadido a procesado_final: {os.path.basename(event.src_path)}")

        except PermissionError:
            print(f"[ERROR] Acceso denegado tras 10 intentos: {os.path.basename(event.src_path)}")
        except Exception as e:
            print(f"[ERROR] Procesando {os.path.basename(event.src_path)}: {e}")

if __name__ == "__main__":
    WATCH_DIR  = r"C:\datos\Practicas"
    OUTPUT_DIR = r"C:\datos\Practicas\salida"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    handler = ExcelHandler(WATCH_DIR, OUTPUT_DIR)

    # --- Procesar cualquier .xlsx que ya esté en la carpeta al iniciar ---
    for f in os.listdir(WATCH_DIR):
        if f.lower().endswith(".xlsx"):
            event = type("E", (), {"src_path": os.path.join(WATCH_DIR, f)})
            handler.on_created(event)
    # --------------------------------------------------------------------------------

    observer = Observer()
    observer.schedule(handler, WATCH_DIR, recursive=False)
    observer.start()
    print(f"Vigilando {WATCH_DIR}…")

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
