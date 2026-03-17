import pandas as pd
import re
import os
from datetime import datetime

# =========================
# LOG
# =========================

def log(msg):
    with open("logs/log.txt", "a") as f:
        f.write(f"{datetime.now()} - {msg}\n")


# =========================
# LIMPIEZA
# =========================

def limpiar_columnas(df):
    df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")
    return df


# =========================
# PARSEAR CUENTA
# =========================

def separar_cuenta(texto):
    if pd.isna(texto):
        return None, None, None

    texto = str(texto).replace(" ", "")
    match = re.match(r"([A-Za-z]+)(\d{1,3})(\d+)", texto)

    if match:
        return match.group(1), match.group(2), match.group(3)

    return None, None, None


# =========================
# CATALOGO
# =========================

def cargar_catalogo():

    df = pd.read_csv("base/catalogo.csv")
    df = limpiar_columnas(df)

    df["fecha_de_creacion"] = pd.to_datetime(df["fecha_de_creacion"])

    fechas = df["fecha_de_creacion"].sort_values().unique()

    if len(fechas) < 2:
        raise Exception("No hay suficientes versiones para comparar")

    df_old = df[df["fecha_de_creacion"] == fechas[-2]]
    df_new = df[df["fecha_de_creacion"] == fechas[-1]]

    return df_old, df_new


# =========================
# AJUSTES
# =========================

def procesar_ajustes():

    df = pd.read_excel("archivos/anexo_ajustes.xlsx",
                       sheet_name="detalle ajuste",
                       header=4)

    df = limpiar_columnas(df)

    df = df.rename(columns={"cuentas": "cuenta"})
    df = df[["cuenta", "descripcion"]].dropna()

    df["categoria_3"] = "Ajuste"

    return df


# =========================
# ACTIVOS
# =========================

def procesar_activos():

    hojas = ["resto de activos", "resto de contingentes"]
    lista = []

    for hoja in hojas:

        df = pd.read_excel("archivos/resto_activos.xlsx",
                           sheet_name=hoja,
                           header=None)

        df = df.iloc[4:, [1, 2]]
        df.columns = ["cuenta_raw", "descripcion"]

        df["cuenta"] = df["cuenta_raw"].where(
            df["cuenta_raw"].astype(str).str.contains(r"\d")
        )

        df["cuenta"] = df["cuenta"].ffill()
        df = df[df["descripcion"].notna()]

        df[["categoria_1","categoria_2","cuenta_limpia"]] = df["cuenta"].apply(
            lambda x: pd.Series(separar_cuenta(x))
        )

        df["cuenta"] = df["cuenta_limpia"]
        df["categoria_3"] = "resto de activos"

        lista.append(df[["cuenta","descripcion","categoria_1","categoria_2","categoria_3"]])

    return pd.concat(lista, ignore_index=True)


# =========================
# MAIN
# =========================

def main():

    try:

        log("Inicio proceso")

        df_old, df_new = cargar_catalogo()

        ajustes = procesar_ajustes()
        activos = procesar_activos()

        keys = ["cuenta","categoria_1","categoria_2","categoria_3"]

        df_old = df_old[keys + ["monto"]]
        df_new = df_new[keys + ["monto"]]

        comparacion = df_old.merge(
            df_new,
            on=keys,
            how="outer",
            suffixes=("_old","_new"),
            indicator=True
        )

        cambios = comparacion[
            (comparacion["_merge"] == "both") &
            (comparacion["monto_old"] != comparacion["monto_new"])
        ]

        if not cambios.empty:

            cambios.to_excel("base/reporte_final.xlsx", index=False)
            log("Cambios detectados")

        else:
            log("Sin cambios")

        log("Fin proceso")

    except Exception as e:

        log(f"ERROR: {str(e)}")


if __name__ == "__main__":
    main()