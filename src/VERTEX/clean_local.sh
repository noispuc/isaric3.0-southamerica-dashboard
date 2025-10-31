echo "🧹 Cleaning up stopped containers and dangling images..."

# Remove containers parados
docker container prune -f

# Remove imagens sem tag (dangling)
docker image prune -f

# remover tudo, inclusive volumes e redes não usadas
docker system prune -a -f --volumes