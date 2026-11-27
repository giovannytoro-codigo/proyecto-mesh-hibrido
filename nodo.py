import asyncio
from datetime import datetime, timezone
import json
import os
import socket
import sqlite3
import uuid
from cryptography.fernet import Fernet, InvalidToken
import httpx
from zeroconf import ServiceBrowser, ServiceInfo, ServiceListener, Zeroconf
from zeroconf.asyncio import AsyncZeroconf
import websockets

NODE_ID = "nodo-portatil-1"  # En el segundo portátil usa "nodo-portatil-2"
PUERTO = 8765
TIPO_SERVICIO = "_meshmsg._tcp.local."
DB_PATH = "mensajes.db"

SYNC_URL = os.getenv("SYNC_URL", "http://127.0.0.1:8000/sync/mensajes")
SYNC_INTERVALO = 10  # segundos entre intentos de sincronización

CLAVE_FERNET = b"mP8xQ2vL9nR4tY7wZ1bC5dF8gH0jK3mN6pQ9rS2tV4w="
CIPHER = Fernet(CLAVE_FERNET)

VISTO_IDS = set()
VECINOS = {}  # {node_id: {"ip": "192.168.x.x", "port": 8765}}
CLIENTES_WS: dict[str, websockets.ServerConnection] = {}
DB_CONN: sqlite3.Connection | None = None


def cifrar_payload(texto_plano: str) -> str:
    return CIPHER.encrypt(texto_plano.encode("utf-8")).decode("utf-8")


def descifrar_payload(token_cifrado: str) -> tuple[str, bool]:
    try:
        plano = CIPHER.decrypt(token_cifrado.encode("utf-8")).decode("utf-8")
        return plano, True
    except (InvalidToken, ValueError):
        return token_cifrado, False


def inicializar_db(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mensajes (
            msg_id TEXT PRIMARY KEY,
            direccion TEXT NOT NULL,
            origen TEXT NOT NULL,
            destino TEXT NOT NULL,
            ttl INTEGER NOT NULL,
            payload TEXT NOT NULL,
            estado TEXT NOT NULL,
            creado_en TEXT NOT NULL,
            actualizado_en TEXT NOT NULL,
            sincronizado INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    columnas = {fila[1] for fila in conn.execute("PRAGMA table_info(mensajes)").fetchall()}
    if "sincronizado" not in columnas:
        conn.execute("ALTER TABLE mensajes ADD COLUMN sincronizado INTEGER NOT NULL DEFAULT 0")
    conn.commit()
    return conn


def cargar_ids_vistos(conn: sqlite3.Connection) -> set[str]:
    cursor = conn.execute("SELECT msg_id FROM mensajes")
    return {fila[0] for fila in cursor.fetchall()}


def guardar_mensaje_db(
    conn: sqlite3.Connection,
    msg_id: str,
    direccion: str,
    origen: str,
    destino: str,
    ttl: int,
    payload: str,
    estado: str,
) -> None:
    ahora = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT OR IGNORE INTO mensajes (
            msg_id, direccion, origen, destino, ttl, payload, estado,
            creado_en, actualizado_en, sincronizado
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
        """,
        (msg_id, direccion, origen, destino, ttl, payload, estado, ahora, ahora),
    )
    conn.commit()


def actualizar_estado_mensaje(conn: sqlite3.Connection, msg_id: str, nuevo_estado: str) -> None:
    ahora = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        UPDATE mensajes
        SET estado = ?, actualizado_en = ?, sincronizado = 0
        WHERE msg_id = ?
        """,
        (nuevo_estado, ahora, msg_id),
    )
    conn.commit()


def obtener_pendientes_sync(conn: sqlite3.Connection, limite: int = 50) -> list[dict]:
    cursor = conn.execute(
        """
        SELECT msg_id, direccion, origen, destino, ttl, payload, estado, creado_en, actualizado_en
        FROM mensajes
        WHERE sincronizado = 0
        ORDER BY creado_en ASC
        LIMIT ?
        """,
        (limite,),
    )
    filas = cursor.fetchall()
    return [
        {
            "msg_id": r[0],
            "nodo_reporta": NODE_ID,
            "direccion": r[1],
            "origen": r[2],
            "destino": r[3],
            "ttl": r[4],
            "payload": r[5],
            "estado": r[6],
            "creado_en": r[7],
            "actualizado_en": r[8],
        }
        for r in filas
    ]


def marcar_sincronizados(conn: sqlite3.Connection, msg_ids: list[str]) -> None:
    if not msg_ids:
        return
    ahora = datetime.now(timezone.utc).isoformat()
    placeholders = ",".join("?" for _ in msg_ids)
    conn.execute(
        f"""
        UPDATE mensajes
        SET sincronizado = 1, actualizado_en = ?
        WHERE msg_id IN ({placeholders})
        """,
        [ahora, *msg_ids],
    )
    conn.commit()


def consultar_historial(conn: sqlite3.Connection, limite: int = 10) -> list[tuple]:
    cursor = conn.execute(
        """
        SELECT direccion, origen, destino, ttl, estado, sincronizado, payload, creado_en
        FROM mensajes
        ORDER BY creado_en DESC
        LIMIT ?
        """,
        (limite,),
    )
    return cursor.fetchall()


def obtener_ip_local() -> str:
    """Obtiene la IPv4 local usada para salir hacia la red."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


class OyenteVecinos(ServiceListener):
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop

    def _procesar_servicio(self, zc: Zeroconf, service_type: str, name: str, accion: str) -> None:
        info = zc.get_service_info(service_type, name, timeout=2000)
        if not info or not info.addresses:
            return

        ip = socket.inet_ntoa(info.addresses[0])
        puerto = info.port
        props = {
            k.decode("utf-8", errors="ignore"): v.decode("utf-8", errors="ignore")
            for k, v in info.properties.items()
        }
        vecino_id = props.get("node_id") or name.split(".")[0]

        if vecino_id == NODE_ID:
            return

        if accion in ("agregado", "actualizado"):
            VECINOS[vecino_id] = {"ip": ip, "port": puerto}
            self.loop.call_soon_threadsafe(
                print, f"\n[📡 VECINO DETECTADO] {vecino_id} -> {ip}:{puerto}"
            )
        elif accion == "eliminado":
            VECINOS.pop(vecino_id, None)
            self.loop.call_soon_threadsafe(
                print, f"\n[📴 VECINO DESCONECTADO] {vecino_id}"
            )

    def add_service(self, zc: Zeroconf, service_type: str, name: str) -> None:
        self._procesar_servicio(zc, service_type, name, "agregado")

    def update_service(self, zc: Zeroconf, service_type: str, name: str) -> None:
        self._procesar_servicio(zc, service_type, name, "actualizado")

    def remove_service(self, zc: Zeroconf, service_type: str, name: str) -> None:
        vecino_id = name.split(".")[0]
        if vecino_id != NODE_ID and vecino_id in VECINOS:
            VECINOS.pop(vecino_id, None)
            self.loop.call_soon_threadsafe(
                print, f"\n[📴 VECINO DESCONECTADO] {vecino_id}"
            )


async def registrar_zeroconf(loop: asyncio.AbstractEventLoop, ip_local: str):
    aiozc = AsyncZeroconf()
    nombre_instancia = f"{NODE_ID}.{TIPO_SERVICIO}"
    info = ServiceInfo(
        TIPO_SERVICIO,
        nombre_instancia,
        addresses=[socket.inet_aton(ip_local)],
        port=PUERTO,
        properties={"node_id": NODE_ID.encode("utf-8")},
        server=f"{NODE_ID}.local.",
    )
    await aiozc.async_register_service(info)
    oyente = OyenteVecinos(loop)
    navegador = ServiceBrowser(aiozc.zeroconf, TIPO_SERVICIO, oyente)
    print(f"📡 Anunciado en mDNS como '{nombre_instancia}' en {ip_local}:{PUERTO}")
    return aiozc, info, navegador


async def enviar_a_vecino(
    vecino_id: str, datos_vecino: dict, paquete: dict, actualizar_ack: bool = True
) -> None:
    uri = f"ws://{datos_vecino['ip']}:{datos_vecino['port']}"
    try:
        async with websockets.connect(uri, open_timeout=3) as websocket:
            await websocket.send(json.dumps(paquete))
            confirmacion = await websocket.recv()
            print(f"[✅ ACK de {vecino_id} ({uri})]: {confirmacion}")
            if actualizar_ack and DB_CONN is not None:
                actualizar_estado_mensaje(DB_CONN, paquete["msg_id"], "ack_recibido")
    except Exception as exc:
        print(f"❌ Error enviando a {vecino_id} ({uri}): {exc}")


async def reenviar_paquete(paquete: dict, excluir_ids: set[str]) -> None:
    ttl_actual = int(paquete.get("ttl", 1))
    if ttl_actual <= 1:
        print(f"⏹️ [TTL agotado] No se reenvía msg_id={paquete.get('msg_id')} (TTL={ttl_actual}).")
        return

    paquete_reenvio = dict(paquete)
    paquete_reenvio["ttl"] = ttl_actual - 1

    candidatos = {
        vid: datos
        for vid, datos in VECINOS.items()
        if vid != NODE_ID and vid not in excluir_ids
    }
    if not candidatos:
        print(f"ℹ️ Sin vecinos alternativos para reenviar msg_id={paquete.get('msg_id')}.")
        return

    print(
        f"🔁 [REENVÍO] msg_id={paquete.get('msg_id')} hacia {list(candidatos.keys())} "
        f"con nuevo TTL={paquete_reenvio['ttl']}"
    )
    tareas = [
        enviar_a_vecino(vid, datos, paquete_reenvio, actualizar_ack=False)
        for vid, datos in candidatos.items()
    ]
    await asyncio.gather(*tareas)


async def escuchar_nodo(websocket):
    async for raw in websocket:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            print(f"\n[📥 TEXTO PLANO]: {raw}")
            await websocket.send(json.dumps({"ack": "texto-recibido", "desde": NODE_ID}))
            continue

        msg_id = data.get("msg_id")
        if msg_id in VISTO_IDS:
            print(f"\n[♻️ DUPLICADO DESCARTADO] msg_id={msg_id}")
            await websocket.send(json.dumps({"ack": msg_id, "desde": NODE_ID, "dup": True}))
            continue
        if msg_id:
            VISTO_IDS.add(msg_id)

        origen = data.get("origen", "desconocido")
        destino = data.get("destino", "broadcast")
        ttl = int(data.get("ttl", 1))
        cifrado = bool(data.get("cifrado", False))

        if cifrado and "payload_cifrado" in data:
            texto_mostrado, ok_descifrado = descifrar_payload(str(data["payload_cifrado"]))
            etiqueta_cifrado = "🔐 descifrado" if ok_descifrado else "⚠️ error-clave"
        else:
            texto_mostrado = str(data.get("payload", ""))
            etiqueta_cifrado = "🔓 plano"

        es_para_mi = destino in (NODE_ID, "broadcast")
        estado_db = "recibido" if es_para_mi else "en_transito"

        if DB_CONN is not None and msg_id:
            guardar_mensaje_db(
                DB_CONN,
                msg_id=msg_id,
                direccion="entrante",
                origen=origen,
                destino=destino,
                ttl=ttl,
                payload=texto_mostrado,
                estado=estado_db,
            )

        if es_para_mi:
            print(f"\n[📥 {origen} -> {destino} | TTL={ttl} | {etiqueta_cifrado}]: {texto_mostrado}")
        else:
            print(f"\n[🔀 TRÁNSITO {origen} -> {destino} | TTL={ttl} | {etiqueta_cifrado}]")

        await websocket.send(json.dumps({"ack": msg_id, "desde": NODE_ID}))

        if destino != NODE_ID:
            await reenviar_paquete(data, excluir_ids={origen})


async def iniciar_servidor():
    servidor = await websockets.serve(escuchar_nodo, "0.0.0.0", PUERTO)
    print(f"🟢 [{NODE_ID}] escuchando en puerto {PUERTO}...")
    await servidor.wait_closed()


async def consola_envio() -> None:
    await asyncio.sleep(2)
    while True:
        objetivo = await asyncio.to_thread(
            input,
            "\nDestino ('broadcast', ID de vecino, 'vecinos', o 'historial'): ",
        )
        objetivo = objetivo.strip()

        if not objetivo or objetivo.lower() == "vecinos":
            if not VECINOS:
                print("ℹ️ Aún no hay vecinos descubiertos en la red local.")
            else:
                for vid, info in VECINOS.items():
                    print(f"  - {vid}: {info['ip']}:{info['port']}")
            continue

        if objetivo.lower() == "historial":
            if DB_CONN is None:
                print("⚠️ Base de datos no inicializada.")
                continue
            filas = consultar_historial(DB_CONN, limite=10)
            if not filas:
                print("ℹ️ No hay mensajes almacenados en mensajes.db.")
            else:
                print("\n--- Últimos mensajes en mensajes.db ---")
                for direccion, origen, destino, ttl, estado, sync_flag, payload, creado_en in filas:
                    etiqueta_sync = "sync=OK" if sync_flag else "sync=PEND"
                    print(
                        f"[{creado_en}] ({direccion} | {estado} | {etiqueta_sync} | TTL={ttl}) "
                        f"{origen} -> {destino}: {payload}"
                    )
            continue

        texto = await asyncio.to_thread(input, "Mensaje: ")
        texto = texto.strip() or "Hola desde red Mesh híbrida"

        ttl_entrada = await asyncio.to_thread(input, "TTL inicial (Enter para 3): ")
        try:
            ttl_valor = max(1, int(ttl_entrada.strip())) if ttl_entrada.strip() else 3
        except ValueError:
            ttl_valor = 3

        msg_id = str(uuid.uuid4())
        token_cifrado = cifrar_payload(texto)

        paquete = {
            "msg_id": msg_id,
            "origen": NODE_ID,
            "destino": objetivo,
            "ttl": ttl_valor,
            "cifrado": True,
            "payload_cifrado": token_cifrado,
        }
        VISTO_IDS.add(msg_id)

        if DB_CONN is not None:
            guardar_mensaje_db(
                DB_CONN,
                msg_id=msg_id,
                direccion="saliente",
                origen=NODE_ID,
                destino=objetivo,
                ttl=ttl_valor,
                payload=texto,
                estado="enviado",
            )

        print(f"🔒 Token enviado por red: {token_cifrado[:28]}...")

        if objetivo == "broadcast":
            if not VECINOS:
                print("⚠️ No hay vecinos conectados para hacer broadcast.")
                continue
            tareas = [
                enviar_a_vecino(vid, datos, paquete) for vid, datos in VECINOS.items()
            ]
            await asyncio.gather(*tareas)
        elif objetivo in VECINOS:
            await enviar_a_vecino(objetivo, VECINOS[objetivo], paquete)
        else:
            print(f"⚠️ El vecino '{objetivo}' no está en VECINOS. Escribe 'vecinos' para ver activos.")


async def ciclo_sincronizacion() -> None:
    await asyncio.sleep(3)
    async with httpx.AsyncClient(timeout=5.0) as client:
        while True:
            try:
                if DB_CONN is not None:
                    pendientes = obtener_pendientes_sync(DB_CONN, limite=50)
                    if pendientes:
                        resp = await client.post(SYNC_URL, json={"mensajes": pendientes})
                        resp.raise_for_status()
                        ids_ok = resp.json().get("ids_confirmados", [])
                        marcar_sincronizados(DB_CONN, ids_ok)
                        print(f"\n[☁️ SYNC OK] {len(ids_ok)} mensaje(s) enviados a PostgreSQL.")
            except Exception as exc:
                print(f"\n[⏳ SYNC PENDIENTE] Sin enlace a FastAPI/PostgreSQL ({exc}). Se reintentará.")
            await asyncio.sleep(SYNC_INTERVALO)


async def main() -> None:
    global DB_CONN, VISTO_IDS

    DB_CONN = inicializar_db(DB_PATH)
    VISTO_IDS = cargar_ids_vistos(DB_CONN)
    print(f"🗄️ Base de datos '{DB_PATH}' lista ({len(VISTO_IDS)} IDs previos cargados).")

    bucle = asyncio.get_running_loop()
    ip_local = obtener_ip_local()
    aiozc, info, navegador = await registrar_zeroconf(bucle, ip_local)

    try:
        await asyncio.gather(
            iniciar_servidor(),
            consola_envio(),
            ciclo_sincronizacion(),
        )
    finally:
        navegador.cancel()
        await aiozc.async_unregister_service(info)
        await aiozc.async_close()
        if DB_CONN is not None:
            DB_CONN.close()
        print("🛑 Servicio Zeroconf y base de datos detenidos limpiamente.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Nodo detenido.")