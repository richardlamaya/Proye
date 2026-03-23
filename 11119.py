class ProcesadorDatos:
    def __init__(self, df):
        self.df = df

    def procesar_cuentas(self, columna_cuenta, columna_saldo):
        if columna_cuenta not in self.df.columns:
            raise ValueError(f"La columna '{columna_cuenta}' no existe")

        # =========================
        # LIMPIEZA
        # =========================
        self.df[columna_cuenta] = (
            self.df[columna_cuenta]
            .astype(str)
            .str.replace(r"\D", "", regex=True)
            .str.strip()
        )

        # =========================
        # FILTRO 14 DIGITOS
        # =========================
        self.df = self.df[self.df[columna_cuenta].str.len() == 14]

        # =========================
        # POSICIONES
        # =========================
        self.df["pos9"] = self.df[columna_cuenta].str[8]
        self.df["pos12"] = self.df[columna_cuenta].str[11]
        self.df["tipo"] = self.df[columna_cuenta].str[0]

        # =========================
        # MONEDA
        # =========================
        def obtener_moneda(x):
            if x == "1":
                return "NIO"
            elif x == "2":
                return "NIX"
            elif x == "3":
                return "USD"
            else:
                return "OTRO"

        self.df["moneda"] = self.df["pos9"].apply(obtener_moneda)

        # =========================
        # VALIDACIÓN USD
        # =========================
        self.df["valida_usd"] = True

        condicion = (self.df["pos9"] == "3") & (self.df["pos12"] != "8")
        self.df.loc[condicion, "valida_usd"] = False

        # =========================
        # CATEGORIA
        # =========================
        self.df["categoria"] = self.df["tipo"].map({
            "1": "ACTIVO",
            "2": "PASIVO"
        })

        # =========================
        # RUBRO Y SUBRUBRO
        # =========================
        self.df["rubro"] = self.df[columna_cuenta].str[:2]
        self.df["subrubro"] = self.df[columna_cuenta].str[:4]

        # =========================
        # SALDOS
        # =========================
        self.df[columna_saldo] = self.df[columna_saldo].fillna(0)

        self.df["saldo_nio"] = self.df.apply(
            lambda x: x[columna_saldo] if x["moneda"] == "NIO" else 0,
            axis=1
        )

        self.df["saldo_usd"] = self.df.apply(
            lambda x: x[columna_saldo] if x["moneda"] == "USD" else 0,
            axis=1
        )

        print("✔ Procesamiento completo aplicado")

        return self.df

    def eliminar_columnas(self, columnas):
        columnas_validas = [col for col in columnas if col in self.df.columns]
        self.df = self.df.drop(columns=columnas_validas)
        return self.df