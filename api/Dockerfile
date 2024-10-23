# Dockerfile para FastAPI
FROM python:3.9

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia los archivos necesarios al contenedor
COPY ./api /app

# Instala las dependencias del archivo requirements.txt
RUN pip install --upgrade pip setuptools
RUN pip install --no-cache-dir -r /app/requirements.txt

RUN ls -la /app

# Comando por defecto para iniciar FastAPI
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]