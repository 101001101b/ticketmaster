#!/bin/bash

# Verificar que se ha pasado la nueva IP como argumento
if [ -z "$1" ]; then
    echo "Error: Debes pasar la nueva IP privada."
    echo "Uso: $0 <NUEVA_IP_PRIVADA>"
    exit 1
fi

NEW_IP=$1
OLD_IP_PATTERN="172\.31\.[0-9]\{1,3\}\.[0-9]\{1,3\}"

echo "=== Actualizando archivos de configuración a la IP: $NEW_IP ==="

# 1. Buscar y actualizar docker-compose.yml si existe en el directorio actual
if [ -f "docker-compose.yml" ]; then
    # Exclusión para evitar que la máquina de Infra pise su propia IP de Redis/Rabbit
    # Solo reemplaza si la IP que encuentra es diferente a la nueva
    sed -i "s/$OLD_IP_PATTERN/$NEW_IP/g" docker-compose.yml
    echo "[✔] docker-compose.yml actualizado."
fi

# 2. Buscar y actualizar nginx.conf si existe en el directorio actual
if [ -f "nginx.conf" ]; then
    sed -i "s/$OLD_IP_PATTERN/$NEW_IP/g" nginx.conf
    echo "[✔] nginx.conf actualizado."
fi

# 3. Reiniciar los contenedores para aplicar cambios de red de forma limpia
echo "=== Reiniciando contenedores de Docker ==="
docker compose down && docker compose up -d

echo "=== ¡Proceso completado con éxito! ==="