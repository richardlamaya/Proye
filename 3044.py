# src/lector.py
import os
from datetime import datetime

class LectorArchivos:
    def __init__(self, ruta):
        self.ruta = ruta

    def obtener_archivos(self):
        """
        Retorna lista de tuplas (nombre_archivo, fecha) ordenadas por fecha
        """
        archivos = []
        for nombre in os.listdir(self.ruta):
            if nombre.endswith(".xlsx"):
                try:
                    # Se asume formato: portafolio_YYYYMMDD.xlsx
                    fecha_str = nombre.split("_")[-1].replace(".xlsx", "")
                    fecha = datetime.strptime(fecha_str, "%Y%m%d")
                    archivos.append((nombre, fecha))
                except:
                    print(f"No se pudo extraer fecha de: {nombre}")
        return sorted(archivos, key=lambda x: x[1])
        
        
        
# src/limpiador.py
import pandas as pd

class LimpiadorDataFrame:
    def limpiar(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia el DataFrame:
        - Elimina columnas vacías
        - Quita espacios en nombres de columnas
        - Quita espacios en strings
        """
        df = df.dropna(axis=1, how='all')
        df.columns = df.columns.str.strip()

        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.strip()
        return df
        
        
        
    # src/comparador.py
import pandas as pd

class Comparador:
    def comparar(self, df_actual: pd.DataFrame, df_hist: pd.DataFrame) -> pd.DataFrame:
        """
        Compara df_actual con df_hist y devuelve:
        - Filas nuevas
        - Filas con cambios en columnas críticas
        Usa SM + isin como clave única
        """
        if df_hist.empty:
            return df_actual.copy()

        columnas = [col for col in df_actual.columns if col not in ['fecha_archivo']]

        df_merge = df_actual.merge(
            df_hist,
            on=['SM', 'isin'],
            how='left',
            suffixes=('', '_hist'),
            indicator=True
        )

        # Filas nuevas
        df_nuevas = df_merge[df_merge['_merge'] == 'left_only'].drop(columns=['_merge'])

        # Filas existentes con cambios
        comunes = df_merge[df_merge['_merge'] == 'both'].copy()
        cambios = []

        for col in columnas:
            if col + '_hist' in comunes.columns:
                cambios_col = comunes[comunes[col] != comunes[col + '_hist']]
                cambios.append(cambios_col)

        if cambios:
            df_cambios = pd.concat(cambios, ignore_index=True)
            df_cambios = df_cambios.drop_duplicates(subset=['SM','isin'])
            df_errores = pd.concat([df_nuevas, df_cambios], ignore_index=True)
        else:
            df_errores = df_nuevas

        return df_errores
        
        
                    # src/exportador.py
import os
import pandas as pd

class Exportador:
    def exportar(self, df: pd.DataFrame, ruta_salida: str):
        """
        Exporta DataFrame a Excel en la ruta especificada
        """
        carpeta = os.path.dirname(ruta_salida)
        os.makedirs(carpeta, exist_ok=True)
        df.to_excel(ruta_salida, index=False)
        print(f"Archivo exportado en: {ruta_salida}")
        
        
        
        
      import os
import pandas as pd
from datetime import timedelta

from src.lector import LectorArchivos
from src.limpiador import LimpiadorDataFrame
from src.comparador import Comparador
from src.exportador import Exportador

# =========================
# CONFIGURACIÓN
# =========================
ruta = "data/"
carpeta_resultado = "resultado/"
columnas_validar = ["SM", "tasa", "fecha de vencimiento", "isin", "fecha de apertura", "valor original"]

# =========================
# INSTANCIAS
# =========================
lector = LectorArchivos(ruta)
limpiador = LimpiadorDataFrame()
comparador = Comparador()
exportador = Exportador()

# =========================
# FLUJO PRINCIPAL
# =========================
archivos = lector.obtener_archivos()
dfs = []

for nombre, fecha in archivos:
    path = os.path.join(ruta, nombre)
    try:
        df = pd.read_excel(path)
    except Exception as e:
        print(f"Error leyendo {nombre}: {e}")
        continue

    # Validar columnas críticas
    faltantes = [c for c in columnas_validar if c not in df.columns]
    if faltantes:
        print(f"{nombre} tiene columnas faltantes: {faltantes}")
        continue

    df = limpiador.limpiar(df)
    df["fecha_archivo"] = fecha
    dfs.append(df)

if not dfs:
    print("No hay archivos válidos para procesar.")
else:
    df_actual = dfs[-1]
    df_hist = pd.concat(dfs[:-1], ignore_index=True) if len(dfs) > 1 else pd.DataFrame()

    if not df_hist.empty:
        fecha_max = df_actual['fecha_archivo'].iloc[0]
        fecha_min = fecha_max - timedelta(days=20)
        df_hist = df_hist[(df_hist['fecha_archivo'] >= fecha_min) & (df_hist['fecha_archivo'] < fecha_max)]

    errores = comparador.comparar(df_actual, df_hist)

    nombre_salida = f"errores_{df_actual['fecha_archivo'].iloc[0].strftime('%Y%m%d')}.xlsx"
    ruta_salida = os.path.join(carpeta_resultado, nombre_salida)
    exportador.exportar(errores, ruta_salida)  