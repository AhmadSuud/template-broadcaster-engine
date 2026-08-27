# Menggunakan image Python versi slim
FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Menginstal dependensi sistem C++
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libc-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Menyalin dan menginstal dependensi Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Menyalin seluruh kode aplikasi
COPY . .

# Menjalankan aplikasi
CMD ["python", "main.py"]