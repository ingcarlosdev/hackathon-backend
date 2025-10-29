from validators import validar_csv


if __name__ == "__main__":
    ruta_csv = "/home/esn8der/Documentos/PC/hackathon/plantaciones.csv"  # coloca aquí tu archivo CSV
    resultado = validar_csv(ruta_csv)
    
    # Mostrar el resultado de manera legible
    import json
    print(json.dumps(resultado, indent=2, ensure_ascii=False))
