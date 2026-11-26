import asyncio
import json
import socket
import uuid
from zeroconf import ServiceBrowser, ServiceInfo, ServiceListener, Zeroconf
from zeroconf.asyncio import AsyncZeroconf
import websockets

NODE_ID = "nodo-portatil-1"  # En el segundo portátil usa "nodo-portatil-2"
PUERTO = 8765
TIPO_SERVICIO = "_meshmsg._tcp.local."

VISTO_IDS = set()
VECINOS = {}  # {node_id: {"ip": "192.168.x.x", "port": 8765}}


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
            continue
        if msg_id:
            VISTO_IDS.add(msg_id)

        origen = data.get("origen", "desconocido")
        destino = data.get("destino", "broadcast")
        ttl = data.get("ttl", 1)
        payload = data.get("payload", "")

        print(f"\n[📥 {origen} -> {destino} | TTL={ttl}]: {payload}")
        await websocket.send(json.dumps({"ack": msg_id, "desde": NODE_ID}))


async def iniciar_servidor():
    servidor = await websockets.serve(escuchar_nodo, "0.0.0.0", PUERTO)
    print(f"🟢 [{NODE_ID}] escuchando en puerto {PUERTO}...")
    await servidor.wait_closed()


async def enviar_a_vecino(vecino_id: str, datos_vecino: dict, paquete: dict) -> None:
    uri = f"ws://{datos_vecino['ip']}:{datos_vecino['port']}"
    try:
        async with websockets.connect(uri, open_timeout=3) as websocket:
            await websocket.send(json.dumps(paquete))
            confirmacion = await websocket.recv()
            print(f"[✅ ACK de {vecino_id} ({uri})]: {confirmacion}")
    except Exception as exc:
        print(f"❌ Error enviando a {vecino_id} ({uri}): {exc}")


async def consola_envio() -> None:
    await asyncio.sleep(2)
    while True:
        objetivo = await asyncio.to_thread(
            input,
            "\nDestino ('broadcast', ID de vecino, o 'vecinos' para listar): ",
        )
        objetivo = objetivo.strip()

        if not objetivo or objetivo.lower() == "vecinos":
            if not VECINOS:
                print("ℹ️ Aún no hay vecinos descubiertos en la red local.")
            else:
                for vid, info in VECINOS.items():
                    print(f"  - {vid}: {info['ip']}:{info['port']}")
            continue

        texto = await asyncio.to_thread(input, "Mensaje: ")
        texto = texto.strip() or "Hola desde red Mesh con Zeroconf"

        paquete = {
            "msg_id": str(uuid.uuid4()),
            "origen": NODE_ID,
            "destino": objetivo,
            "ttl": 3,
            "payload": texto,
        }
        VISTO_IDS.add(paquete["msg_id"])

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


async def main() -> None:
    bucle = asyncio.get_running_loop()
    ip_local = obtener_ip_local()
    aiozc, info, navegador = await registrar_zeroconf(bucle, ip_local)

    try:
        await asyncio.gather(
            iniciar_servidor(),
            consola_envio(),
        )
    finally:
        navegador.cancel()
        await aiozc.async_unregister_service(info)
        await aiozc.async_close()
        print("🛑 Servicio Zeroconf detenido limpiamente.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Nodo detenido.")