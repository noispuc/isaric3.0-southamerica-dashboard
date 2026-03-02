#!/bin/bash
IMAGE_NAME="vertex"
PORT=8050
WORKDIR="/app"
ENV_FILE="../../sinan.env"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  source "$ENV_FILE"
  set +a
else
  echo "ERRO: arquivo $ENV_FILE não encontrado" >&2
  exit 1
fi

echo "🐳 Checking if Docker image '$IMAGE_NAME' exists..."

# Verifica se a imagem existe localmente
if [[ "$(docker images -q $IMAGE_NAME 2> /dev/null)" == "" ]]; then
    echo "🔨 Image not found. Building new image..."
    echo "🛠️ Building Docker image..."
    docker build -t $IMAGE_NAME .
else
    echo "✅ Image '$IMAGE_NAME' found locally."
fi

# Executa o container
echo "🐳 Launching Docker container..."
docker run --rm \
  -v "$(pwd)":$WORKDIR \
  -p $PORT:$PORT \
  -w $WORKDIR \
  -e PGUSER=$PGUSER \
  -e PGHOST=$PGHOST \
  -e PGPORT=$PGPORT \
  -e PGPASSWORD=$PGPASSWORD \
  -e PGDATABASE=$PGDATABASE \
  -t $IMAGE_NAME
