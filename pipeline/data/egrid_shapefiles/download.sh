#!/bin/bash
# Download eGRID 2022 subregion shapefile from EPA
# Run this once after cloning — file is too large for git
set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
URL="https://www.epa.gov/system/files/other-files/2024-05/egrid2022_subregions_shapefile.zip"
echo "Downloading eGRID 2022 subregions shapefile..."
curl -L "$URL" -o "$DIR/egrid2022_subregions.zip"
unzip -o "$DIR/egrid2022_subregions.zip" -d "$DIR/"
echo "Done. Load into PostGIS with:"
echo "  shp2pgsql -I -s 4326 $DIR/eGRID2022_Subregions.shp public.egrid_subregions | psql \$DATABASE_URL"
