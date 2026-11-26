import asyncio
import websockets

# 1. Función que actúa como SERVIDOR (Modificada para recibir solo el websocket)
async def escuchar_nodo(websocket):
    async for mensaje in websocket:
        print(f"\n[📥 MENSAJE RECIBIDO]: {mensaje}")
        # Responder de vuelta confirmando recepción
        await websocket.send("Mensaje recibido exitosamente por este nodo.")

async def iniciar_servidor():
    # Inicia el servidor WebSocket en el puerto 8765 escuchando en la red local
    server = await websockets.serve(escuchar_nodo, "0.0.0.0", 8765)
    print("🟢 Nodo iniciado y escuchando en el puerto 8765...")
    await server.wait_closed()

# 2. Función que actúa como CLIENTE (Envía un mensaje a otro nodo)
async def enviar_mensaje(ip_destino):
    await asyncio.sleep(2) # Espera un par de segundos a que el servidor levante
    uri = f"ws://{ip_destino}:8765"
    
    try:
        print(f"🔄 Intentando conectar con el nodo en {uri}...")
        async with websockets.connect(uri) as websocket:
            texto = "¡Hola desde el nodo emisor en la red local!"
            print(f"📤 Enviando: '{texto}'")
            await websocket.send(texto)
            
            # Esperar respuesta del otro nodo
            respuesta = await websocket.recv()
            print(f"[✅ RESPUESTA DEL DESTINO]: {respuesta}")
    except Exception as e:
        print(f"❌ No se pudo conectar con el nodo destino: {e}")

# 3. Función principal que coordina ambas acciones
async def main():
    ip = input("Ingresa la IP a la que deseas enviar un mensaje (escribe '127.0.0.1' para probar contigo mismo): ")
    
    # Ejecuta el servidor y el cliente en paralelo
    await asyncio.gather(
        iniciar_servidor(),
        enviar_mensaje(ip)
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Nodo detenido por el usuario.")