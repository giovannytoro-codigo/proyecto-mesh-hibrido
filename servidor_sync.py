from datetime import datetime, timezone
import os
from fastapi import FastAPI, HTTPException
import psycopg
from pydantic import BaseModel

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/mesh_db",
)

app = FastAPI(title="Mesh Hybrid Sync API")


class MensajeSync(BaseModel):
    msg_id: str
    nodo_reporta: str
    direccion: str
    origen: str
    destino: str
    ttl: int
    payload: str
    estado: str
    creado_en: str
    actualizado_en: str


class LoteSync(BaseModel):
    mensajes: list[MensajeSync]


def inicializar_tabla_pg() -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS mensajes_mesh (
                    msg_id TEXT PRIMARY KEY,
                    nodo_reporta TEXT NOT NULL,
                    direccion TEXT NOT NULL,
                    origen TEXT NOT NULL,
                    destino TEXT NOT NULL,
                    ttl INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    estado TEXT NOT NULL,
                    creado_en TIMESTAMPTZ NOT NULL,
                    actualizado_en TIMESTAMPTZ NOT NULL,
                    sincronizado_en TIMESTAMPTZ NOT NULL
                )
                """
            )
        conn.commit()


@app.on_event("startup")
def al_iniciar() -> None:
    inicializar_tabla_pg()


@app.get("/health")
def salud() -> dict:
    return {"status": "ok"}


@app.post("/sync/mensajes")
def recibir_mensajes(lote: LoteSync) -> dict:
    if not lote.mensajes:
        return {"procesados": 0, "ids_confirmados": []}

    ahora = datetime.now(timezone.utc)
    confirmados: list[str] = []

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                for m in lote.mensajes:
                    cur.execute(
                        """
                        INSERT INTO mensajes_mesh (
                            msg_id, nodo_reporta, direccion, origen, destino,
                            ttl, payload, estado, creado_en, actualizado_en, sincronizado_en
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (msg_id) DO NOTHING
                        """,
                        (
                            m.msg_id,
                            m.nodo_reporta,
                            m.direccion,
                            m.origen,
                            m.destino,
                            m.ttl,
                            m.payload,
                            m.estado,
                            m.creado_en,
                            m.actualizado_en,
                            ahora,
                        ),
                    )
                    confirmados.append(m.msg_id)
            conn.commit()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error en PostgreSQL: {exc}") from exc

    return {"procesados": len(confirmados), "ids_confirmados": confirmados}