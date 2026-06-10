# Guía de Despliegue — TicketMaster

## Prerrequisitos

### 1. Docker y Docker Compose

En cada máquina AWS Academy (Ubuntu):

```bash
# Actualizar paquetes
sudo apt update && sudo apt upgrade -y

# Instalar Docker
sudo apt install -y docker.io

# Iniciar Docker y habilitarlo al arranque
sudo systemctl enable --now docker

# Agregar tu usuario al grupo docker (para no usar sudo)
sudo usermod -aG docker $USER
# Cerrar sesión y volver a entrar, o ejecutar: newgrp docker

# Verificar instalación
docker --version

# Docker Compose v2 ya viene incluido en docker.io
docker compose version
```

### 2. Clonar el repositorio

```bash
git clone <url-del-repositorio> ticketmaster
cd ticketmaster
```

### 3. Puertos necesarios

Cada VM debe tener los siguientes puertos abiertos en su Security Group de AWS:

**VM-A (Infraestructura: Redis, RabbitMQ, NGINX):**
| Puerto | Servicio |
|--------|----------|
| TCP 22 | SSH |
| TCP 80 | NGINX (entrada para benchmarks directos) |
| TCP 5672 | RabbitMQ AMQP |
| TCP 15672 | RabbitMQ Management UI (opcional) |
| TCP 6379 | Redis |

**VM-B (Workers):**
| Puerto | Servicio |
|--------|----------|
| TCP 22 | SSH |
| TCP 8000-8100 | Workers directos |

Usa tu VPC CIDR (ej: `172.31.0.0/16`) como origen para reglas internas,
y tu IP pública para SSH.

---

## Estructura del repositorio

```
ticketmaster/
├── client/
│   ├── b.sh                         # Script de orquestación de benchmarks
│   ├── docker-compose.yml           # Contenedores del cliente (productor)
│   ├── producer/                    # Driver de benchmark
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── producer.py
│   ├── results_consumer/            # Consumidor de resultados RabbitMQ
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── consumer.py
│   ├── benchmarks/                  # Archivos de carga de trabajo
│   └── results/                     # Resultados JSONL
├── infra/                           # Stack de infraestructura (VM-A)
│   ├── docker-compose.yml           # rabbitmq, redis, nginx
│   ├── nginx.conf
│   ├── redis.conf
│   └── ch_ip.sh
├── worker/                          # Workers de tickets (VM-B)
│   ├── docker-compose.yml
│   ├── Dockerfile
│   ├── requirements.txt
│   └── worker.py
└── docs/
    ├── deploy.md                    # Este archivo
    └── specifications.txt           # Especificación original
```

---

## Despliegue en AWS Academy (2 VMs)

### VM-A: Infraestructura (Redis, RabbitMQ, NGINX)

```bash
cd ticketmaster/infra

# Iniciar los servicios
docker compose up -d

# Verificar que estén corriendo
docker compose ps

# Probar que Redis responde
redis-cli -h localhost ping
# Debería responder: PONG

# Probar que RabbitMQ responde
# Abrir en navegador: http://<IP_VM_A>:15672 (guest/guest)
```

### VM-B: Workers

Antes de continuar, actualizar la IP privada de VM-A en los archivos de configuración.
En VM-A, ejecutar el script `ch_ip.sh` con la IP privada de VM-A:

```bash
cd ticketmaster/infra
./ch_ip.sh <IP_PRIVADA_VM_A>
```

Esto actualiza automáticamente `docker-compose.yml` y `nginx.conf` con la IP correcta.

**En VM-B**, copiar la carpeta `worker/` (o clonar el repo) y ejecutar:

```bash
cd ticketmaster/worker

# Modo directo
docker compose up -d --scale worker-direct=4

# Modo indirecto
docker compose up -d --scale worker-indirect=4
```

### Configurar NGINX (Modo Directo)

El script `client/b.sh` configura NGINX automáticamente en cada ejecución:

1. Detecta los puertos activos de los workers en VM-B vía SSH
2. Genera un `nginx.conf` con los upstreams correspondientes
3. Lo sube a VM-A y reinicia NGINX

También puedes hacerlo manualmente editando `infra/nginx.conf`:

```nginx
upstream backend_servers {
    server <IP_PRIVADA_VM_B>:8000;
    server <IP_PRIVADA_VM_B>:8001;  # si hay más workers
}
```

---

## Ejecutar Benchmarks

Todo se lanza desde la máquina local (o la que tenga acceso SSH a ambas VMs).

### Configurar IPs en `client/b.sh`

Editar las variables al inicio del script:

```bash
AWS_INFRA_IP="<IP_PUBLICA_VM_A>"     # IP pública de VM-A
AWS_WORKER_IP="<IP_PUBLICA_VM_B>"    # IP pública de VM-B
AWS_WORKER_PRIV_IP="<IP_PRIVADA_VM_B>" # IP privada de VM-B
```

### Ejecutar

```bash
cd ticketmaster/client

# Benchmarks modo directo
./b.sh direct unnumbered 50 1
./b.sh direct numbered   50 1
./b.sh direct contention 50 1

# Benchmarks modo indirecto
./b.sh indirect unnumbered 50 4
./b.sh indirect numbered   50 4
./b.sh indirect contention 50 4
```

Parámetros: `./b.sh <modo> <tipo> <clientes> <workers>`

El script:
1. Limpia contenedores viejos en VM-B y escala los workers
2. Reconfigura NGINX en VM-A (solo modo directo)
3. Resetea Redis via `POST /reset`
4. Ejecuta el benchmark y guarda resultados en `client/results/`

---

## Escalado Dinámico

Durante una ejecución, se pueden agregar workers:

```bash
# En VM-B:
docker compose up -d --scale worker-direct=8
# NGINX detecta los nuevos puertos automáticamente al reiniciar
```

---

## Reseteo entre ejecuciones

```bash
curl -X POST http://<IP_VM_A>:80/reset
```

El script `b.sh` lo hace automáticamente en cada ejecución.

---

## Benchmarks de Alta Contención

El archivo `benchmark_contention.txt` contiene 20.000 peticiones donde
el 80% de los requests apuntan al 5% de los asientos (hotspot).
Esto genera contención real en Redis con `SETNX`.

---

## Servicios

| Servicio | Descripción |
|----------|-------------|
| `rabbitmq` | Message broker (modo indirecto); Management UI en puerto 15672 |
| `redis` | Backend de consistencia (INCR / SETNX atómicos) |
| `worker-direct` | FastAPI REST worker, escalable, detrás de NGINX |
| `worker-indirect` | RabbitMQ consumer worker, escalable |
| `nginx` | Balanceador para modo directo (puerto 80 → workers) |
| `results_consumer` | Consume cola `ticket_results` y escribe a JSONL |
| `producer-direct` | Driver de benchmark para modo directo |
| `producer-indirect` | Driver de benchmark para modo indirecto |

---

## Notas

- El productor en modo directo cuenta éxitos/fallos desde la respuesta HTTP.
- En modo indirecto, el productor publica fire-and-forget en RabbitMQ y
  luego lee los contadores autoritativos desde Redis (`read_redis_counts`).
- Redis usa `INCR` para entradas sin numerar y `SETNX` para asientos numerados,
  garantizando atomicidad sin necesidad de transacciones.
