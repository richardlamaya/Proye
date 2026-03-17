import pandas as pd
import re

# =====================================
# LIMPIEZA GENERAL
# =====================================

def limpiar_columnas(df):
    df.columns = (
        df.columns
        .str.lower()
        .str.strip()
        .str.replace(" ", "_")
    )
    return df


# =====================================
# PARSEAR CUENTA (PR00123 → PR | 0 | 123)
# =====================================

def separar_cuenta(texto):

    if pd.isna(texto):
        return None, None, None

    texto = str(texto).replace(" ", "")

    match = re.match(r"([A-Za-z]+)(\d{1,3})(\d+)", texto)

    if match:
        return match.group(1), match.group(2), match.group(3)

    return None, None, None


# =====================================
# EXTRACT - CATALOGO
# =====================================

def cargar_catalogo(ruta):

    df = pd.read_csv(ruta)
    df = limpiar_columnas(df)

    df["fecha_de_creacion"] = pd.to_datetime(df["fecha_de_creacion"])

    fechas = df["fecha_de_creacion"].sort_values().unique()

    df_old = df[df["fecha_de_creacion"] == fechas[-2]]
    df_new = df[df["fecha_de_creacion"] == fechas[-1]]

    return df_old, df_new


# =====================================
# TRANSFORM - BALANCE
# =====================================

def procesar_balance(ruta):

    archivo = pd.ExcelFile(ruta)
    lista = []

    for hoja in archivo.sheet_names:

        df = pd.read_excel(ruta, sheet_name=hoja, header=1)
        df = limpiar_columnas(df)

        if "cuenta" in df.columns:

            df = df[["cuenta", "descripcion"]]

            df["cod_moneda"] = hoja
            df["categoria_3"] = None

            lista.append(df)

    return pd.concat(lista, ignore_index=True)


# =====================================
# TRANSFORM - AJUSTES
# =====================================

def procesar_ajustes(ruta):

    df = pd.read_excel(ruta, sheet_name="detalle ajuste", header=4)
    df = limpiar_columnas(df)

    df = df.rename(columns={"cuentas": "cuenta"})

    df = df[["cuenta", "descripcion"]].dropna()

    df["categoria_3"] = "Ajuste"

    return df


# =====================================
# TRANSFORM - ACTIVOS
# =====================================

def procesar_activos(ruta):

    hojas = ["resto de activos", "resto de contingentes"]
    lista = []

    for hoja in hojas:

        df = pd.read_excel(ruta, sheet_name=hoja, header=None)

        df = df.iloc[4:, [1, 2]]
        df.columns = ["cuenta_raw", "descripcion"]

        df = df.dropna(how="all")

        # detectar cuenta
        df["cuenta"] = df["cuenta_raw"].where(
            df["cuenta_raw"].astype(str).str.contains(r"\d")
        )

        df["cuenta"] = df["cuenta"].ffill()

        df = df[df["descripcion"].notna()]

        # separar PR
        df[["categoria_1","categoria_2","cuenta_limpia"]] = df["cuenta"].apply(
            lambda x: pd.Series(separar_cuenta(x))
        )

        df["cuenta"] = df["cuenta_limpia"]

        df["categoria_3"] = "resto de activos"

        lista.append(df[["cuenta","descripcion","categoria_1","categoria_2","categoria_3"]])

    return pd.concat(lista, ignore_index=True)


# =====================================
# LOAD - PROCESO PRINCIPAL
# =====================================

df_old, df_new = cargar_catalogo("base/catalogo.csv")

balance = procesar_balance("archivos/balance_cierre.xlsx")
ajustes = procesar_ajustes("archivos/anexo_ajustes.xlsx")
activos = procesar_activos("archivos/resto_activos.xlsx")

nuevo = pd.concat([balance, ajustes, activos], ignore_index=True)


# =====================================
# COMPARACION (AUDITORIA)
# =====================================

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

# cambios de monto
cambios = comparacion[
    (comparacion["_merge"] == "both") &
    (comparacion["monto_old"] != comparacion["monto_new"])
]

# clasificar origen
def origen(row):
    if row["categoria_3"] == "Ajuste":
        return "Ir a archivo AJUSTES"
    elif row["categoria_3"] == "resto de activos":
        return "Ir a ACTIVOS"
    else:
        return "Balance"

cambios["origen"] = cambios.apply(origen, axis=1)


# =====================================
# REPORTE FINAL
# =====================================

with pd.ExcelWriter("base/reporte_final.xlsx") as writer:

    cambios.to_excel(writer, sheet_name="cambios_monto", index=False)

print("ETL + Auditoría completado")