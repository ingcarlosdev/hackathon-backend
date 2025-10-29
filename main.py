from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import io
from typing import List, Dict, Any
import uvicorn

app = FastAPI(title="Excel to API CSV Reader", version="1.0.0")

# Configurar CORS para permitir requests desde el frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # URL del frontend Next.js
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "Excel to API CSV Reader Backend"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    """
    Endpoint para subir y procesar archivos CSV
    """
    try:
        # Verificar que el archivo sea CSV
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="El archivo debe ser un CSV")
        
        # Leer el contenido del archivo
        contents = await file.read()
        
        # Convertir a DataFrame de pandas
        df = pd.read_csv(io.StringIO(contents.decode('utf-8')))
        
        # Convertir DataFrame a diccionario para respuesta JSON
        data = df.to_dict('records')
        
        return {
            "filename": file.filename,
            "rows": len(data),
            "columns": list(df.columns),
            "data": data
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error procesando el archivo: {str(e)}")

@app.post("/upload-excel")
async def upload_excel(file: UploadFile = File(...)):
    """
    Endpoint para subir y procesar archivos Excel
    """
    try:
        # Verificar que el archivo sea Excel
        if not (file.filename.endswith('.xlsx') or file.filename.endswith('.xls')):
            raise HTTPException(status_code=400, detail="El archivo debe ser un Excel (.xlsx o .xls)")
        
        # Leer el contenido del archivo
        contents = await file.read()
        
        # Convertir a DataFrame de pandas
        df = pd.read_excel(io.BytesIO(contents))
        
        # Convertir DataFrame a diccionario para respuesta JSON
        data = df.to_dict('records')
        
        return {
            "filename": file.filename,
            "rows": len(data),
            "columns": list(df.columns),
            "data": data
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error procesando el archivo: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
