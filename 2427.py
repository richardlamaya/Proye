clases/archivo_csv.py
import pandas as pd

class ArchivoCSV:
    def __init__(self, ruta, fecha=None, libro=None):
        self.ruta = ruta
        self.fecha = fecha
        self.libro = libro

    def cargar(self):
        """Carga un CSV y devuelve un DataFrame."""
        df = pd.read_csv(self.ruta, sep=None, engine='python', encoding='latin1')
        print(f"✔ CSV cargado correctamente desde: {self.ruta}")
        return df
clases/procesador.py
class ProcesadorDatos:
    def __init__(self, df):
        self.df = df.copy()  # no muta el DataFrame original

    def filtrar_cuenta_14(self, columna_cuenta):
        """Filtra filas donde la columna indicada empieza con '14'."""
        if columna_cuenta not in self.df.columns:
            raise ValueError(f"La columna '{columna_cuenta}' no existe. "
                             f"Columnas disponibles: {list(self.df.columns)}")

        self.df[columna_cuenta] = self.df[columna_cuenta].astype(str)
        self.df = self.df[self.df[columna_cuenta].str.startswith("14")].copy()
        print(f"✔ Filtro aplicado: {len(self.df)} filas con cuentas que empiezan con '14'")
        return self.df

    def eliminar_columnas(self, columnas):
        """Elimina columnas si existen, ignora las que no están."""
        columnas_validas = [col for col in columnas if col in self.df.columns]
        columnas_no_encontradas = [col for col in columnas if col not in self.df.columns]

        if columnas_no_encontradas:
            print(f"⚠ Columnas no encontradas (ignoradas): {columnas_no_encontradas}")

        self.df = self.df.drop(columns=columnas_validas)
        print(f"✔ Columnas eliminadas: {columnas_validas}")
        return self.df

    def obtener_df(self):
        return self.df
clases/pipeline.py
from clases.archivo_csv import ArchivoCSV
from clases.procesador import ProcesadorDatos


class PipelineETL:
    def __init__(self, ruta, columna_cuenta, columnas_drop, salida, fecha=None, libro=None):
        self.ruta = ruta
        self.fecha = fecha
        self.libro = libro
        self.columna_cuenta = columna_cuenta
        self.columnas_drop = columnas_drop
        self.salida = salida

    def ejecutar(self):
        """Ejecuta el pipeline ETL completo. Relanza excepciones para visibilidad."""
        # EXTRAER
        archivo = ArchivoCSV(self.ruta, self.fecha, self.libro)
        df = archivo.cargar()

        # TRANSFORMAR
        procesador = ProcesadorDatos(df)
        df = procesador.filtrar_cuenta_14(self.columna_cuenta)
        df = procesador.eliminar_columnas(self.columnas_drop)

        # CARGAR
        df.to_csv(self.salida, index=False, encoding='utf-8')
        print(f"✔ Archivo guardado en: {self.salida}")

        return df
clases/__init__.py
from .archivo_csv import ArchivoCSV
from .procesador import ProcesadorDatos
from .pipeline import PipelineETL

__all__ = ["ArchivoCSV", "ProcesadorDatos", "PipelineETL"]
notebook.ipynb / main.py
import sys
import os

ruta_proyecto = os.path.abspath("..")
if ruta_proyecto not in sys.path:
    sys.path.append(ruta_proyecto)

from clases import PipelineETL

pipeline = PipelineETL(
    ruta="../base/catalogo.csv",
    fecha="2026-03-20",
    libro="Diario",
    columna_cuenta="numero_cuenta",
    columnas_drop=["col1", "col2", "col3"],
    salida="../resultado.csv"
)

try:
    df_final = pipeline.ejecutar()
    print(f"\nResultado: {df_final.shape[0]} filas, {df_final.shape[1]} columnas")
    df_final.head()
except ValueError as e:
    print(f"❌ Error de validación: {e}")
except FileNotFoundError:
    print("❌ No se encontró el archivo CSV. Verifica la ruta.")
except Exception as e:
    print(f"❌ Error inesperado: {e}")