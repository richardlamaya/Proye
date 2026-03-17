import pandas as pd

# =========================
# CONFIG
# =========================

ruta_catalogo = "base/catalogo.csv"
reporte = "base/reporte_trazabilidad.xlsx"

# =========================
# CARGAR DATA
# =========================

df = pd.read_csv(ruta_catalogo)

df.columns = df.columns.str.lower().str.replace(" ", "_")

df["fecha_de_creacion"] = pd.to_datetime(df["fecha_de_creacion"])

fechas = sorted(df["fecha_de_creacion"].unique())

df_old = df[df["fecha_de_creacion"] == fechas[-2]]
df_new = df[df["fecha_de_creacion"] == fechas[-1]]

# =========================
# CLAVE
# =========================

key = ["cuenta","categoria_1","categoria_2","categoria_3"]

campos = [
    "descripcion",
    "cod_moneda",
    "monto"
]

# =========================
# MERGE BASE
# =========================

merge = df_old.merge(
    df_new,
    on=key,
    how="outer",
    suffixes=("_old","_new"),
    indicator=True
)

# =========================
# DETECTAR CAMBIOS DETALLADOS
# =========================

lista_cambios = []

for _, row in merge.iterrows():

    estado = row["_merge"]

    if estado == "both":

        for campo in campos:

            old = row.get(f"{campo}_old")
            new = row.get(f"{campo}_new")

            if pd.isna(old) and pd.isna(new):
                continue

            if old != new:

                lista_cambios.append({
                    "cuenta": row["cuenta"],
                    "categoria_1": row["categoria_1"],
                    "categoria_2": row["categoria_2"],
                    "categoria_3": row["categoria_3"],
                    "campo": campo,
                    "valor_anterior": old,
                    "valor_nuevo": new,
                    "tipo_cambio": "MODIFICADO"
                })

    elif estado == "right_only":

        lista_cambios.append({
            "cuenta": row["cuenta"],
            "categoria_1": row["categoria_1"],
            "categoria_2": row["categoria_2"],
            "categoria_3": row["categoria_3"],
            "campo": "REGISTRO",
            "valor_anterior": None,
            "valor_nuevo": "NUEVO",
            "tipo_cambio": "NUEVO"
        })

    elif estado == "left_only":

        lista_cambios.append({
            "cuenta": row["cuenta"],
            "categoria_1": row["categoria_1"],
            "categoria_2": row["categoria_2"],
            "categoria_3": row["categoria_3"],
            "campo": "REGISTRO",
            "valor_anterior": "EXISTIA",
            "valor_nuevo": None,
            "tipo_cambio": "ELIMINADO"
        })


df_cambios = pd.DataFrame(lista_cambios)

# =========================
# CLASIFICAR ORIGEN
# =========================

def origen(cat3):

    if cat3 == "Ajuste":
        return "Ir a archivo AJUSTES"
    elif cat3 == "resto de activos":
        return "Ir a ACTIVOS"
    else:
        return "BALANCE"

df_cambios["origen"] = df_cambios["categoria_3"].apply(origen)

df_cambios["fecha_revision"] = pd.Timestamp.today()

# =========================
# EXPORTAR
# =========================

df_cambios.to_excel(reporte, index=False)

print("Reporte de trazabilidad generado")