# app/main.py
import os
from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from app.validators import validar_csv  # 👈 tu función personalizada
import httpx
import logging
from dotenv import load_dotenv

load_dotenv()  # carga variables del .env
logging.basicConfig(level=logging.INFO)

# ⚙️ Inicializar la aplicación FastAPI
app = FastAPI(title="Geo-Validador API", version="1.0")

# 🔐 Configuración CORS
origins = [
    "http://localhost:3000",  # Frontend local (Next.js)
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SIOMA_API_BASE = os.getenv("SIOMA_API_BASE")
SIOMA_API_KEY = os.getenv("SIOMA_API_KEY")
SIOMA_SPOTS_BASE = os.getenv("SIOMA_SPOTS_BASE", os.getenv("SIOMA_API_BASE", "https://api.sioma.dev/api/v1"))

# 🔹 Endpoint para obtener todas las fincas
@app.get("/fincas/")
async def get_fincas():
    try:
        headers = {
            "Authorization": f"{SIOMA_API_KEY}",
            "Content-Type": "application/json",
            "tipo-sujetos": "[1]"
        }
        async with httpx.AsyncClient() as client:
            res = await client.get(f"{SIOMA_API_BASE}/4/usuarios/sujetos", headers=headers)
            res.raise_for_status()
            data = res.json()  # Esto está bien, no requiere await aquí
        logging.info(f"Fincas recibidas: {data}")
        return {"status": "ok", "fincas": data}
    except httpx.RequestError as e:
        logging.error(f"Error de request: {e}")
        return {"status": "error", "message": str(e)}
    except httpx.HTTPStatusError as e:
        logging.error(f"Error HTTP: {e.response.status_code} - {e.response.text}")
        return {"status": "error", "message": str(e)}

@app.get("/lotes/{finca_id}")
async def get_lotes(finca_id: str):
    """
    Devuelve solo los lotes que pertenecen a la finca seleccionada.
    Se obtiene toda la lista de lotes desde la API de Sioma y se filtra por finca_id.
    """
    try:
        headers = {
            "Authorization": f"{SIOMA_API_KEY}",
            "Content-Type": "application/json",
            "tipo-sujetos": "[3]"  # Lotes
        }
        async with httpx.AsyncClient() as client:
            res = await client.get(f"{SIOMA_API_BASE}/4/usuarios/sujetos", headers=headers)
            res.raise_for_status()
            all_lotes = res.json()

        # Filtrar lotes por finca_id
        lotes_filtrados = [lote for lote in all_lotes if str(lote.get("finca_id")) == str(finca_id)]

        return {"status": "ok", "lotes": lotes_filtrados}

    except Exception as e:
        return {"status": "error", "message": str(e)}




# 💾 Endpoint para validar CSV
@app.post("/validar-csv/")
async def validar_csv_endpoint(file: UploadFile = File(...)):
    try:
        resultado = validar_csv(file.file)
        return resultado
    except Exception as e:
        return {"status": "error", "message": str(e)}


# 🚀 Envío a Sioma si el archivo está limpio
@app.post("/sioma/enviar")
async def enviar_a_sioma(
    file: UploadFile = File(...),
    finca_id: str | None = Form(None),
    lote_id: str | None = Form(None),
):
    """
    Revalida el CSV y si no hay errores, envía los datos a Sioma.
    Por ahora hace un passthrough simulado y responde ok.
    """
    try:
        # 1) Validar entrada
        resultado = validar_csv(file.file)
        if resultado.get("status") != "ok":
            raise HTTPException(status_code=400, detail=resultado.get("message", "Validación fallida"))
        if len(resultado.get("errores", [])) > 0:
            raise HTTPException(status_code=400, detail="El archivo contiene errores; no se puede enviar a Sioma")

        if not finca_id:
            raise HTTPException(status_code=400, detail="finca_id es requerido para el envío a Sioma")

        # 2) Transformar coordenadas validadas al CSV requerido por Sioma
        # Campos requeridos: nombre_spot,lat,lng,lote_id,linea,posicion,nombre_planta,finca_id
        from io import StringIO, BytesIO
        import csv

        output = StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "nombre_spot",
            "lat",
            "lng",
            "lote_id",
            "linea",
            "posicion",
            "nombre_planta",
            "finca_id",
        ])

        coordenadas = resultado.get("coordenadas", [])
        # Si no se pasó lote_id, permitimos que el archivo contenga múltiples lotes? En esta UI solemos seleccionar un solo lote
        # Usamos el lote_id del form si se proporcionó; de lo contrario, intentamos tomarlo desde cada fila "lote" si son ids
        for c in coordenadas:
            try:
                lat = float(c.get("latitud")) if "latitud" in c else float(c.get("lat" , c.get("Latitud", c.get("Lat", 0))))
            except Exception:
                lat = float(c.get("Latitud"))
            try:
                lng = float(c.get("longitud")) if "longitud" in c else float(c.get("lng", c.get("Longitud", c.get("Lng", 0))))
            except Exception:
                lng = float(c.get("Longitud"))

            linea_val = int(float(c.get("linea"))) if isinstance(c.get("linea"), (int, float, str)) else 0
            posicion_val = int(float(c.get("palma"))) if isinstance(c.get("palma"), (int, float, str)) else 0

            lote_id_row = lote_id or str(c.get("lote", ""))
            if not lote_id_row:
                raise HTTPException(status_code=400, detail="lote_id es requerido: proporcione en el form o en el archivo")

            nombre_spot = f"L{lote_id_row}L{linea_val}S{posicion_val}"
            nombre_planta = f"L{lote_id_row}L{linea_val}P{posicion_val}"

            writer.writerow([
                nombre_spot,
                f"{lat}",
                f"{lng}",
                int(lote_id_row),
                linea_val,
                posicion_val,
                nombre_planta,
                int(finca_id),
            ])

        csv_text = output.getvalue()
        output.close()
        csv_bytes = csv_text.encode("utf-8")

        # 3) Enviar a Sioma como multipart/form-data con el archivo CSV construido
        headers = {
            "Authorization": f"{SIOMA_API_KEY}",
        }
        files = {"file": ("Spots.csv", csv_bytes, "text/csv")}

        async with httpx.AsyncClient(timeout=60.0) as client:
            url = f"{SIOMA_SPOTS_BASE.rstrip('/')}/spots/upload"
            res = await client.post(url, headers=headers, files=files)
            # Algunos endpoints devuelven 200 aún para errores; parsear con tolerancia
            if not res.content:
                data = {"status": "error", "message": "Sin respuesta", "status_code": res.status_code}
            else:
                try:
                    data = res.json()
                except ValueError:
                    data = {"status": "unknown", "status_code": res.status_code, "text": res.text}

        return {"status": "ok", "message": "Solicitud enviada a Sioma", "sioma_response": data}
    except HTTPException:
        raise
    except Exception as e:
        return {"status": "error", "message": str(e)}
    

