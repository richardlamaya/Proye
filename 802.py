import os
from datetime import datetime

class LectorArchivos:

    def __init__(self, ruta):
        self.ruta = ruta

    def obtener_archivos(self):
        archivos = []

        for f in os.listdir(self.ruta):
            if f.endswith(".xlsx") and "lcr" in f.lower():
                try:
                    fecha_str = f.split("_")[-1].replace(".xlsx", "")
                    fecha = datetime.strptime(fecha_str, "%Y-%m-%d")
                    archivos.append((f, fecha))
                except:
                    print(f"Archivo ignorado: {f}")

        archivos.sort(key=lambda x: x[1])
        return archivos[-21:]  # últimos 20 + actualice 
import pandas as pd

class LimpiadorDataFrame:

    def __init__(self):
        pass

    def limpiar(self, df):
        df.columns = df.columns.str.strip().str.lower()

        df = df.rename(columns={
            "fecha de vencimiento": "fecha_vencimiento",
            "fecha vencimiento": "fecha_vencimiento",
            "fecha de apertura": "fecha_apertura",
            "fecha apertura": "fecha_apertura"
        })

        return df
        
        
        
        
        
import pandas as pd

class Comparador:

    def __init__(self):
        self.id_col = "sm"
        self.columnas = [
            "tasa",
            "fecha_vencimiento",
            "isin",
            "fecha_apertura"
        ]

    def comparar(self, df_actual, df_hist):

        df = df_actual.merge(
            df_hist,
            on=self.id_col,
            how="left",
            indicator=True,
            suffixes=("_actual", "_hist")
        )

        errores = []

        # nuevos o eliminados
        base = df[df["_merge"] != "both"]
        errores.append(base)

        # cambios por columna
        for col in self.columnas:
            col_a = f"{col}_actual"
            col_h = f"{col}_hist"

            if col_a in df.columns and col_h in df.columns:

                if "fecha" in col:
                    df[col_a] = pd.to_datetime(df[col_a], errors="coerce")
                    df[col_h] = pd.to_datetime(df[col_h], errors="coerce")

                diff = df[df[col_a] != df[col_h]].copy()
                diff["columna_error"] = col

                errores.append(diff)

        return pd.concat(errores).drop_duplicates()
        
        
        
        
        
        
        
        
        from datetime import datetime

class Exportador:

    def exportar(self, df):

        if df is None or df.empty:
            print("No hay diferencias")
            return

        nombre = f"errores_lcr_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"

        df.to_excel(nombre, index=False)

        print(f"Archivo generado: {nombre}")
        
        
        
        
        
        
        
        
        
       import pandas as pd
import os

from src.lector import LectorArchivos
from src.limpiador import LimpiadorDataFrame
from src.comparador import Comparador
from src.exportador import Exportador

# =========================
# CONFIG
# =========================
ruta = "data/"

# =========================
# INSTANCIAS
# =========================
lector = LectorArchivos(ruta)
limpiador = LimpiadorDataFrame()
comparador = Comparador()
exportador = Exportador()

# =========================
# FLUJO
# =========================
archivos = lector.obtener_archivos()

dfs = []

for nombre, fecha in archivos:
    path = os.path.join(ruta, nombre)
    df = pd.read_excel(path)
    df = limpiador.limpiar(df)
    df["fecha_archivo"] = fecha
    dfs.append(df)

df_actual = dfs[-1]
df_hist = pd.concat(dfs[:-1], ignore_index=True)

errores = comparador.comparar(df_actual, df_hist)

exportador.exportar(errores)