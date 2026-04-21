FROM python:3.12-slim

# System dependencies for geopandas/shapely
RUN apt-get update && apt-get install -y \
    libgdal-dev \
    gdal-bin \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py fetch_data.py dma_polygon_map.csv dma_polygons.geojson zip_to_dma.csv ./

EXPOSE 8080

CMD python fetch_data.py && gunicorn app:server --bind 0.0.0.0:8080 --workers 1 --timeout 120
