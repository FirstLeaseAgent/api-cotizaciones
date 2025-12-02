FROM python:3.10-slim

# Evitar que la instalación de libreoffice tarde tanto
ENV DEBIAN_FRONTEND=noninteractive

# Instalar solo LO necesario de LibreOffice para convertir docx→pdf
RUN apt-get update && \
    apt-get install -y --no-install-recommends libreoffice-writer libreoffice-common && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Permitir que Render arranque rápido
HEALTHCHECK --interval=10s --timeout=3s \
    CMD curl -f http://localhost:10000/ || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "10000"]