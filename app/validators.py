import pandas as pd

def validar_csv(file) -> dict:
    try:
        df = pd.read_csv(file, sep=None, engine="python")  # Detecta coma o tab
        df.columns = df.columns.str.strip().str.capitalize()

        columnas_requeridas = {"Lote", "Linea", "Palma", "Longitud", "Latitud"}
        if not columnas_requeridas.issubset(df.columns):
            return {
                "status": "error",
                "message": f"El CSV debe contener las columnas: {', '.join(columnas_requeridas)}"
            }

        errores = []
        coordenadas = []
        filas_con_errores = set()  # Para rastrear filas que deben excluirse

        df = df.reset_index(drop=True)

        # 1️⃣ Verificar tipos y datos faltantes
        for i, row in df.iterrows():
            fila = int(i) + 1
            lote = row.get("Lote")
            linea = row.get("Linea")
            palma = row.get("Palma")
            longitud = row.get("Longitud")
            latitud = row.get("Latitud")

            tiene_error = False

            # Validar faltantes
            if any(pd.isna(x) for x in [lote, linea, palma, longitud, latitud]):
                errores.append({
                    "tipo": "dato_faltante",
                    "descripcion": f"Fila {fila}: falta uno o más datos requeridos.",
                    "fila": fila
                })
                tiene_error = True

            # Validar tipos
            if not tiene_error:
                for campo, valor in [("Linea", linea), ("Palma", palma), ("Longitud", longitud), ("Latitud", latitud)]:
                    try:
                        float(valor)
                    except (ValueError, TypeError):
                        errores.append({
                            "tipo": "dato_invalido",
                            "descripcion": f"Fila {fila}: el campo '{campo}' no es numérico ({valor}).",
                            "fila": fila
                        })
                        tiene_error = True
                        break

            # Solo agregar a coordenadas si no tiene errores básicos
            if not tiene_error:
                coordenadas.append({
                    "lote": lote,
                    "linea": linea,
                    "palma": palma,
                    "longitud": longitud,
                    "latitud": latitud,
                    "_fila": fila  # Guardamos la fila para referencia
                })
            else:
                filas_con_errores.add(fila)

        # 2️⃣ Coordenadas duplicadas → agrupar todas las filas con la misma coordenada
        duplicadas = df[df.duplicated(subset=["Longitud", "Latitud"], keep=False)]
        if not duplicadas.empty:
            grupos = duplicadas.groupby(["Longitud", "Latitud"]).groups
            filas_duplicadas = set()
            # Agrupar por lote para mejor formato de error
            coordenadas_por_lote = {}
            for (long, lat), idxs in grupos.items():
                filas = [int(i + 1) for i in idxs]
                filas_duplicadas.update(filas)
                # Obtener los lotes afectados
                lotes_afectados = df.loc[idxs, "Lote"].unique()
                cantidad = len(filas)
                
                # Agrupar errores por lote cuando sea posible
                for lote in lotes_afectados:
                    if lote not in coordenadas_por_lote:
                        coordenadas_por_lote[lote] = []
                    coordenadas_por_lote[lote].append({
                        "coordenada": (long, lat),
                        "cantidad": cantidad,
                        "filas": filas
                    })
            
            # Crear errores agrupados por lote
            for lote, coords in coordenadas_por_lote.items():
                total_coords_dup = len(coords)
                errores.append({
                    "tipo": "coordenada_repetida",
                    "descripcion": f"{total_coords_dup} coordenada(s) repetida(s) en lote {lote}.",
                    "lote": str(lote),
                    "filas": filas_duplicadas,
                    "cantidad": total_coords_dup
                })
            
            # Excluir coordenadas duplicadas de la lista válida
            coordenadas = [c for c in coordenadas if c["_fila"] not in filas_duplicadas]

        # 3️⃣ Inconsistencias dentro del lote:
        #    a) Dentro de un mismo lote no deben repetirse líneas de palma
        lineas_duplicadas_por_lote = df[df.duplicated(subset=["Lote", "Linea"], keep=False)]
        if not lineas_duplicadas_por_lote.empty:
            grupos = lineas_duplicadas_por_lote.groupby(["Lote", "Linea"]).groups
            filas_lineas_duplicadas = set()
            for (lote, linea), idxs in grupos.items():
                filas = [int(i + 1) for i in idxs]
                filas_lineas_duplicadas.update(filas)
                # Contar cuántas líneas repetidas hay en este lote
                cantidad = len(filas)
                errores.append({
                    "tipo": "linea_repetida_en_lote",
                    "descripcion": f"{cantidad} líneas repetidas en lote {lote} (Línea {linea}).",
                    "lote": str(lote),
                    "linea": str(linea),
                    "filas": filas,
                    "cantidad": cantidad
                })
            
            # Excluir filas con líneas duplicadas de la lista válida
            coordenadas = [c for c in coordenadas if c["_fila"] not in filas_lineas_duplicadas]

        #    b) Dentro de una línea no deben repetirse posiciones de palma
        posiciones_duplicadas_por_linea = df[df.duplicated(subset=["Lote", "Linea", "Palma"], keep=False)]
        if not posiciones_duplicadas_por_linea.empty:
            grupos = posiciones_duplicadas_por_linea.groupby(["Lote", "Linea", "Palma"]).groups
            filas_posiciones_duplicadas = set()
            for (lote, linea, palma), idxs in grupos.items():
                filas = [int(i + 1) for i in idxs]
                filas_posiciones_duplicadas.update(filas)
                # Contar cuántas posiciones repetidas hay
                cantidad = len(filas)
                errores.append({
                    "tipo": "posicion_repetida_en_linea",
                    "descripcion": f"{cantidad} posiciones repetidas en lote {lote}, línea {linea} (Posición {palma}).",
                    "lote": str(lote),
                    "linea": str(linea),
                    "palma": str(palma),
                    "filas": filas,
                    "cantidad": cantidad
                })
            
            # Excluir filas con posiciones duplicadas de la lista válida
            coordenadas = [c for c in coordenadas if c["_fila"] not in filas_posiciones_duplicadas]

        # 4️⃣ Coordenadas fuera de rango (solo si son numéricas válidas)
        filas_fuera_rango = set()
        try:
            df["Latitud_f"] = df["Latitud"].astype(float)
            df["Longitud_f"] = df["Longitud"].astype(float)

            fuera_rango = df[
                (df["Latitud_f"] < -90) | (df["Latitud_f"] > 90) |
                (df["Longitud_f"] < -180) | (df["Longitud_f"] > 180)
            ]
            # Agrupar por lote
            fuera_rango_por_lote = fuera_rango.groupby("Lote")
            for lote, grupo in fuera_rango_por_lote:
                filas_lote = []
                for i, row in grupo.iterrows():
                    fila = int(i + 1)
                    filas_fuera_rango.add(fila)
                    filas_lote.append(fila)
                
                cantidad = len(filas_lote)
                errores.append({
                    "tipo": "coordenada_fuera_rango",
                    "descripcion": f"{cantidad} coordenada(s) fuera de rango en lote {lote}.",
                    "lote": str(lote),
                    "filas": filas_lote,
                    "cantidad": cantidad
                })
            
            # Excluir coordenadas fuera de rango de la lista válida
            coordenadas = [c for c in coordenadas if c["_fila"] not in filas_fuera_rango]
        except Exception:
            pass  # si no se pueden convertir, ya se reportó antes como inválido

        # 5️⃣ Lotes inválidos: Todos los registros deben tener un lote válido según la finca seleccionada
        # NOTA: Esta validación requiere conexión a la API de Sioma para obtener los lotes válidos
        # Por ahora, solo preparamos la estructura. Se implementará cuando se integre la selección de finca.
        # lotes_invalidos = validar_lotes_contra_api(finca_id, df["Lote"].unique())
        # if lotes_invalidos:
        #     for lote in lotes_invalidos:
        #         filas_lote = df[df["Lote"] == lote].index.tolist()
        #         filas = [int(i + 1) for i in filas_lote]
        #         cantidad = len(filas)
        #         errores.append({
        #             "tipo": "lote_invalido",
        #             "descripcion": f"{cantidad} registro(s) con lote inválido: {lote}.",
        #             "lote": str(lote),
        #             "filas": filas,
        #             "cantidad": cantidad
        #         })

        # Remover el campo temporal _fila de las coordenadas válidas
        for c in coordenadas:
            c.pop("_fila", None)

        return {
            "status": "ok",
            "coordenadas": coordenadas,
            "errores": errores
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}
