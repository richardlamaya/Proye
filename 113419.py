import pandas as pd
import re

# =========================
# LIMPIAR
# =========================

def limpiar_columnas(df):
    df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")
    return df


# =========================
# PARSEAR CUENTA ACTIVOS
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
# 1 CATALOGO ACTUAL (CSV)
# =========================

df_catalogo = pd.read_csv("base/catalogo.csv")
df_catalogo = limpiar_columnas(df_catalogo)

df_catalogo["fecha_de_creacion"] = pd.to_datetime(df_catalogo["fecha_de_creacion"])

ultima_fecha = df_catalogo["fecha_de_creacion"].max()

df_actual = df_catalogo[df_catalogo["fecha_de_creacion"] == ultima_fecha]


# =========================
# 2 ETL DESDE EXCEL
# =========================

# -------- BALANCE --------
def procesar_balance():

    archivo = pd.ExcelFile("archivos/balance_cierre.xlsx")
    lista = []

    for hoja in archivo.sheet_names:

        df = pd.read_excel(archivo, sheet_name=hoja, header=1)
        df = limpiar_columnas(df)

        if "cuenta" in df.columns:

            df = df[["cuenta","descripcion","monto"]]

            df["categoria_3"] = None
            df["cod_moneda"] = hoja

            lista.append(df)

    return pd.concat(lista, ignore_index=True)


# -------- AJUSTES --------
def procesar_ajustes():

    df = pd.read_excel(
        "archivos/anexo_ajustes.xlsx",
        sheet_name="detalle ajuste",
        header=4
    )

    df = limpiar_columnas(df)

    df = df.rename(columns={"cuentas":"cuenta"})

    df = df[["cuenta","descripcion","monto"]].dropna()

    df["categoria_3"] = "Ajuste"

    return df


# -------- ACTIVOS --------
def procesar_activos():

    hojas = ["resto de activos", "resto de contingentes"]
    lista = []

    for hoja in hojas:

        df = pd.read_excel(
            "archivos/resto_activos.xlsx",
            sheet_name=hoja,
            header=None
        )

        df = df.iloc[4:, [1, 2, 7]]  # descripción + monto (H)
        df.columns = ["cuenta_raw","descripcion","monto"]

        df["cuenta"] = df["cuenta_raw"].where(
            df["cuenta_raw"].astype(str).str.contains(r"\d")
        )

        df["cuenta"] = df["cuenta"].ffill()

        df = df[df["descripcion"].notna()]

        df[["categoria_1","categoria_2","cuenta"]] = df["cuenta"].apply(
            lambda x: pd.Series(separar_cuenta(x))
        )

        df["categoria_3"] = "resto de activos"

        lista.append(df[["cuenta","descripcion","monto","categoria_1","categoria_2","categoria_3"]])

    return pd.concat(lista, ignore_index=True)


# ejecutar ETL
balance = procesar_balance()
ajustes = procesar_ajustes()
activos = procesar_activos()

df_nuevo = pd.concat([balance, ajustes, activos], ignore_index=True)


# =========================
# 3 COMPLETAR COLUMNAS
# =========================

# agregar columnas faltantes del CSV
for col in df_catalogo.columns:
    if col not in df_nuevo.columns:
        df_nuevo[col] = None


# =========================
# 4 COMPARACION
# =========================

keys = ["cuenta","categoria_1","categoria_2","categoria_3","cod_moneda"]

merge = df_actual.merge(
    df_nuevo,
    on=keys,
    how="outer",
    suffixes=("_old","_new"),
    indicator=True
)

# cambios de monto
cambios = merge[
    (merge["_merge"] == "both") &
    (merge["monto_old"] != merge["monto_new"])
]


# =========================
# 5 ACTUALIZAR SOLO CAMBIOS
# =========================

if not cambios.empty:

    print("CAMBIOS DETECTADOS")

    df_actualizado = df_actual.copy()

    for _, row in cambios.iterrows():

        filtro = (
            (df_actualizado["cuenta"] == row["cuenta"]) &
            (df_actualizado["categoria_3"] == row["categoria_3"])
        )

        df_actualizado.loc[filtro, "monto"] = row["monto_new"]

    # nueva versión
    nueva_fecha = pd.Timestamp.today()

    df_actualizado["fecha_de_creacion"] = nueva_fecha

    # guardar histórico
    df_final = pd.concat([df_catalogo, df_actualizado], ignore_index=True)

    df_final.to_csv("base/catalogo.csv", index=False)

else:

    print("SIN CAMBIOS")