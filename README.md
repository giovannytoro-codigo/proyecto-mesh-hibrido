## Arquitectura del Sistema Mesh Híbrido

```mermaid
flowchart TB
    subgraph MOVIL["Capa Móvil (PWA Offline-First · iPhone / Android)"]
        CEL1["📱 Celular: Juan / Pedro<br/>localStorage + Cola Pendiente + Auto-Recon"]
    end

    subgraph MALLA["Capa Mesh Local (LAN / Hotspot)"]
        subgraph NODO1["💻 Portátil 1 · nodo-portatil-1"]
            WS1["WebSocket Server (:8765)<br/>VECINOS + CLIENTES_WS"]
            MDNS1["AsyncZeroconf / mDNS<br/>_meshmsg._tcp.local."]
            CORE1["Fernet Cipher + TTL + VISTO_IDS<br/>Reenvío Multi-Salto + ACK"]
            DB1[("SQLite: mensajes.db<br/>sincronizado = 0 / 1")]
        end

        subgraph NODO2["💻 Portátil 2 · nodo-portatil-2"]
            WS2["WebSocket Server (:8765)<br/>VECINOS"]
            MDNS2["AsyncZeroconf / mDNS<br/>_meshmsg._tcp.local."]
            CORE2["Fernet Cipher + TTL + VISTO_IDS<br/>Reenvío Multi-Salto + ACK"]
            DB2[("SQLite: mensajes.db<br/>sincronizado = 0 / 1")]
        end
    end

    subgraph NUBE["Capa Centralizada (Enlace Periódico)"]
        API["⚡ FastAPI<br/>POST /sync/mensajes"]
        PG[("🐘 PostgreSQL: mesh_db<br/>Tabla: mensajes_mesh")]
    end

    CEL1 <-->|"ws://IP_NODO:8765 (JSON + ACK)"| WS1
    WS1 --> CORE1
    CORE1 <--> DB1
    MDNS1 <-->|"Descubrimiento mDNS"| MDNS2
    CORE1 <-->|"ws://IP_NODO2:8765 (Fernet + TTL)"| WS2
    WS2 --> CORE2
    CORE2 <--> DB2

    DB1 -.->|"ciclo_sincronizacion (HTTP JSON)"| API
    DB2 -.->|"ciclo_sincronizacion (HTTP JSON)"| API
    API -->|"UPSERT / Deduplicación msg_id"| PG
```

## Flujo de Mensajería y Sincronización (Secuencia)

```mermaid
sequenceDiagram
    autonumber
    participant Cel as Celular (Juan)
    participant N1 as nodo-portatil-1
    participant N2 as nodo-portatil-2
    participant Sync as FastAPI + PostgreSQL

    Cel->>Cel: Guarda mensaje en cola local (Pendiente)
    Cel->>N1: WebSocket send (msg_id, origen=juan, destino, ttl)
    N1->>N1: Deduplica (VISTO_IDS) & guarda en mensajes.db
    N1-->>Cel: ACK (msg_id) -> Marca ✓✓ ACK
    alt ttl > 1 y destino != N1
        N1->>N2: Reenvía paquete (ttl - 1, Fernet)
        N2->>N2: Guarda en mensajes.db & responde ACK
        N2-->>N1: ACK
    end
    loop Cada SYNC_INTERVALO (si hay enlace)
        N1->>Sync: POST /sync/mensajes (pendientes sincronizado=0)
        Sync-->>N1: ids_confirmados
        N1->>N1: Marca sincronizado = 1 en SQLite
    end
```




Estructura del Proyecto

nodo.py: Nodo principal de la malla (mDNS, servidor/cliente WebSocket en puerto 8765, cifrado Fernet, persistencia local en SQLite y sincronización en segundo plano).

servidor_sync.py: API central en FastAPI que recibe lotes pendientes y realiza UPSERT idempotente en PostgreSQL.

app_movil/: Cliente móvil PWA (index.html, manifest.json, sw.js) instalable en iPhone y Android con nombres personalizados y cola offline.

Guía Rápida de Arranque

1. Servidor de Sincronización Central (FastAPI + PostgreSQL)

uvicorn servidor_sync:app --host 0.0.0.0 --port 8000


2. Nodos Portátiles en la Malla

En el Portátil 1 (NODE_ID = "nodo-portatil-1"):

python nodo.py


En el Portátil 2 (NODE_ID = "nodo-portatil-2"), apuntando al servidor central:

$env:SYNC_URL="http://IP_SERVIDOR:8000/sync/mensajes"
python nodo.py


3. App Móvil (PWA para iPhone y Android)

Servir la carpeta app_movil en la red local:

cd app_movil
python -m http.server 8080 --bind 0.0.0.0


Luego abrir http://IP_PORTATIL_1:8080 desde el navegador del celular, configurar el nombre de usuario (por ejemplo Juan o Pedro) y la dirección ws://IP_PORTATIL_1:8765 en ⚙️, y seleccionar Agregar a pantalla de inicio.
