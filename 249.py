import pandas as pd
import os
from datetime import datetime

class ControlCambiosLCR:

    def __init__(self, ruta):
        self.ruta = ruta
        self.columna_id = "SM"
        self.columnas_comparar = [
            "tasa",
            "fecha_vencimiento",
            "isin",
            "fecha_apertura"
        ]

        if not os.path.exists(self.ruta):
            raise Exception("La carpeta no existe")

    # =========================
    # OBTENER ARCHIVOS
    # =========================
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
        return archivos

    # =========================
    # LIMPIAR COLUMNAS
    # =========================
    def limpiar_columnas(self, df):
        df.columns = df.columns.str.strip().str.lower()

        # normalizar nombres
        df = df.rename(columns={
            "fecha de vencimiento": "fecha_vencimiento",
            "fecha vencimiento": "fecha_vencimiento",
            "fecha de apertura": "fecha_apertura",
            "fecha apertura": "fecha_apertura"
        })

        return df

    # =========================
    # CARGAR DATA
    # =========================
    def cargar_datos(self, archivos):
        dfs = []

        for nombre, fecha in archivos:
            path = os.path.join(self.ruta, nombre)

            try:
                df = pd.read_excel(path)
                df = self.limpiar_columnas(df)

                # asegurar que existe SM
                if "sm" not in df.columns:
                    print(f"{nombre} no tiene columna SM")
                    continue

                df["fecha_archivo"] = fecha
                dfs.append(df)

            except Exception as e:
                print(f"Error en {nombre}: {e}")

        return dfs

    # =========================
    # COMPARAR
    # =========================
    def comparar(self):
        archivos = self.obtener_archivos()

        if len(archivos) < 2:
            print("No hay suficientes archivos")
            return None

        archivos = archivos[-21:]  # 20 anteriores + actual

        dfs = self.cargar_datos(archivos)

        df_actual = dfs[-1]
        df_anteriores = pd.concat(dfs[:-1], ignore_index=True)

        # merge por SM
        df_merge = df_actual.merge(
            df_anteriores,
            left_on="sm",
            right_on="sm",
            how="left",
            indicator=True,
            suffixes=("_actual", "_anterior")
        )

        # =========================
        # DETECTAR CAMBIOS
        # =========================
        cambios = df_merge[df_merge["_merge"] != "both"].copy()

        for col in self.columnas_comparar:
            col_actual = f"{col}_actual"
            col_anterior = f"{col}_anterior"

            if col_actual in df_merge.columns and col_anterior in df_merge.columns:

                # manejar fechas correctamente
                if "fecha" in col:
                    df_merge[col_actual] = pd.to_datetime(df_merge[col_actual], errors="coerce")
                    df_merge[col_anterior] = pd.to_datetime(df_merge[col_anterior], errors="coerce")

                df_merge[f"cambio_{col}"] = (
                    df_merge[col_actual] != df_merge[col_anterior]
                )

        # detectar cambios en valores
        columnas_flags = [f"cambio_{c}" for c in self.columnas_comparar]

        cambios_valores = df_merge[df_merge[columnas_flags].any(axis=1)]

        cambios = pd.concat([cambios, cambios_valores]).drop_duplicates()

        return cambios

    # =========================
    # GUARDAR REPORTE
    # =========================
    def guardar_reporte(self, df_cambios):

        if df_cambios is None or df_cambios.empty:
            print("No se detectaron cambios")
            return

        nombre = f"reporte_cambios_lcr_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"

        with pd.ExcelWriter(nombre) as writer:
            df_cambios.to_excel(writer, sheet_name="Cambios", index=False)

        print(f"Reporte generado: {nombre}")


# =========================
# EJECUCIÓN
# =========================
if __name__ == "__main__":

    ruta = "data/"  # 👈 CAMBIA ESTO

    control = ControlCambiosLCR(ruta)

    cambios = control.comparar()

    control.guardar_reporte(cambios)