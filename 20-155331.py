import pandas as pd
import re
from pathlib import Path
from openpyxl.utils import get_column_letter

# =========================
# RUTAS
# =========================
BASE = Path("base")
ARCHIVOS = Path("archivos")

ruta_catalogo = BASE / "catalogo.csv"
ruta_reporte = BASE / "reporte_trazabilidad.xlsx"
ruta_balance = ARCHIVOS / "balance_cierre.xlsx"
ruta_ajustes = ARCHIVOS / "anexo_ajustes.xlsx"
ruta_activos = ARCHIVOS / "resto_activos.xlsx"

# =========================
# FUNCIONES AUXILIARES
# =========================

def detectar_header(df):
    """Detecta fila header aunque haya celdas combinadas"""
    palabras = ["cuenta","descripcion","monto"]
    mejor_fila = None
    mejor_score = 0
    for i in range(min(15, len(df))):
        fila = df.iloc[i].astype(str).str.lower()
        score = sum(any(p in str(v) for v in fila) for p in palabras)
        if score > mejor_score:
            mejor_score = score
            mejor_fila = i
    if mejor_score >= 2:
        return mejor_fila
    return None

def leer_excel(ruta, hoja):
    """Lee excel dinámico detectando header automáticamente"""
    df_raw = pd.read_excel(ruta, sheet_name=hoja, header=None)
    header_row = detectar_header(df_raw)
    if header_row is None:
        return None
    df = df_raw.iloc[header_row+1:].copy()
    df.columns = df_raw.iloc[header_row]
    df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")
    df = df.loc[:, ~df.columns.str.contains("^unnamed")]
    df = df.reset_index()
    df.rename(columns={"index":"fila_excel"}, inplace=True)
    df["fila_excel"] += header_row + 2
    return df

def separar_cuenta(texto):
    """Separa cuenta tipo PR00123 en partes"""
    if pd.isna(texto):
        return None, None, None
    texto = str(texto).replace(" ", "")
    match = re.match(r"([A-Za-z]+)(\d{1,3})(\d+)", texto)
    if match:
        return match.group(1), match.group(2), match.group(3)
    return None, None, None

def comprobar_excel(cuenta, cat3):
    """Busca la cuenta en el Excel correspondiente y devuelve fila y monto"""
    if cat3 == "Ajuste":
        df_excel = leer_excel(ruta_ajustes, "detalle ajuste")
    elif cat3 == "resto de activos":
        df1 = leer_excel(ruta_activos, "resto de activos")
        df2 = leer_excel(ruta_activos, "resto de contingentes")
        df_excel = pd.concat([df1, df2], ignore_index=True)
    else:
        # balance
        df_excel_list = []
        xl = pd.ExcelFile(ruta_balance)
        for hoja in xl.sheet_names:
            df = leer_excel(ruta_balance, hoja)
            if df is not None and "cuenta" in df.columns:
                df_excel_list.append(df)
        if not df_excel_list:
            return None, None
        df_excel = pd.concat(df_excel_list, ignore_index=True)

    if "monto" not in df_excel.columns:
        return None, None

    df_match = df_excel[df_excel["cuenta"] == cuenta]
    if df_match.empty:
        return None, None

    fila = df_match.iloc[0]["fila_excel"]
    monto_excel = df_match.iloc[0]["monto"]

    return fila, monto_excel

# =========================
# LEER CSV HISTÓRICO
# =========================

df = pd.read_csv(ruta_catalogo)
df.columns = df.columns.str.lower().str.replace(" ", "_")
df["fecha_de_creacion"] = pd.to_datetime(df["fecha_de_creacion"])
fechas = sorted(df["fecha_de_creacion"].unique())
df_old = df[df["fecha_de_creacion"] == fechas[-2]]
df_new = df[df["fecha_de_creacion"] == fechas[-1]]

# =========================
# CLAVE Y CAMPOS
# =========================

key = ["cuenta","categoria_1","categoria_2","categoria_3"]
campos = ["descripcion","cod_moneda","monto"]

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
# DETECTAR CAMBIOS CON EXCEL
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

            if campo == "monto":
                fila_excel, valor_excel = comprobar_excel(row["cuenta"], row["categoria_3"])
                if valor_excel is not None and new != valor_excel:
                    new = valor_excel

            if old != new:
                lista_cambios.append({
                    "cuenta": row["cuenta"],
                    "categoria_1": row["categoria_1"],
                    "categoria_2": row["categoria_2"],
                    "categoria_3": row["categoria_3"],
                    "campo": campo,
                    "valor_anterior": old,
                    "valor_nuevo": new,
                    "tipo_cambio": "MODIFICADO",
                    "fila_excel": fila_excel if campo=="monto" else None
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
            "tipo_cambio": "NUEVO",
            "fila_excel": None
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
            "tipo_cambio": "ELIMINADO",
            "fila_excel": None
        })

# =========================
# GENERAR DATAFRAME CAMBIOS
# =========================

df_cambios = pd.DataFrame(lista_cambios)

# =========================
# ORIGEN DEL CAMBIO
# =========================

def origen(cat3):
    if cat3 == "Ajuste":
        return "AJUSTES"
    elif cat3 == "resto de activos":
        return "ACTIVOS"
    else:
        return "BALANCE"

df_cambios["origen"] = df_cambios["categoria_3"].apply(origen)
df_cambios["fecha_revision"] = pd.Timestamp.today()

# =========================
# CELDA EXCEL
# =========================

def obtener_celda(row):
    if row["fila_excel"] is not None:
        return "H" + str(int(row["fila_excel"]))
    return None

df_cambios["celda_excel"] = df_cambios.apply(obtener_celda, axis=1)

# =========================
# EXPORTAR REPORTE
# =========================

df_cambios.to_excel(ruta_reporte, index=False)
print("✅ Reporte de trazabilidad generado:", ruta_reporte)