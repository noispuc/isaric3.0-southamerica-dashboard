#!/bin/bash
IMAGE_NAME="vertex"
PORT=8050

# Dentro do container o repo fica montado em /app
WORKDIR="/app/src/VERTEX"

echo "🐳 Checking if Docker image '$IMAGE_NAME' exists..."

# Verifica se a imagem existe localmente
if [[ "$(docker images -q $IMAGE_NAME 2> /dev/null)" == "" ]]; then
    echo "🔨 Image not found. Building new image..."
    docker build -t $IMAGE_NAME .
else
    echo "✅ Image '$IMAGE_NAME' found locally."
fi

# Executa o container
echo "🐳 Launching Docker container..."
docker run --rm \
  -v "$(pwd)":/app \
  -p $PORT:$PORT \
  -w $WORKDIR \
  -e PYTHONPATH="/app/src/VERTEX" \
  -t $IMAGE_NAME \
  python -m vertex.descriptive_dashboard

