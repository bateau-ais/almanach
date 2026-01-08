"""
Script de test pour publier des messages AIS parsés sur NATS.

Ce script simule la sortie du Parser en publiant une série de messages
ParsedAISMessage sur le topic nova.parsed.
"""

import asyncio
import random
from datetime import datetime, timedelta, timezone

import msgpack
import nats


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


async def publish_test_messages(
    nats_url: str = "nats://localhost:4222",
    topic: str = "nova.parsed",
    total_messages: int = 50,
    mmsi_count: int = 10,
    delay_s: float = 0.05,  # mets 0 pour envoyer "en rafale"
    seed: int | None = 42,
):
    """
    Publie total_messages messages répartis sur mmsi_count MMSI différents.
    """
    if seed is not None:
        random.seed(seed)

    # Connexion à NATS
    print(f"Connexion à NATS ({nats_url})...")
    nc = await nats.connect(nats_url)
    print("✓ Connecté à NATS")

    # Générer une liste de MMSI (10 par défaut)
    # (tu peux remplacer par tes MMSI réels si besoin)
    mmsis = [230000000 + i for i in range(mmsi_count)]

    # "Centre" géographique (Bretagne) + jitter
    base_lat = 48.2
    base_lon = -5.5

    # Temps de départ (UTC) puis on avance un peu à chaque message
    t0 = datetime.now(timezone.utc)

    print(
        f"\nPublication de {total_messages} messages sur {topic} ({mmsi_count} MMSI)...\n"
    )

    for i in range(total_messages):
        mmsi = random.choice(mmsis)

        # Variation position (petits déplacements)
        lat = base_lat + random.uniform(-0.25, 0.25)
        lon = base_lon + random.uniform(-0.35, 0.35)

        # Variation cinématique
        sog = clamp(random.gauss(12.0, 3.5), 0.0, 30.0)  # knots
        cog = random.uniform(0.0, 359.9)  # degrees
        heading = int(clamp(cog + random.uniform(-10, 10), 0, 359))

        # Timestamp qui progresse (ex: +0..2 sec)
        ts = (t0 + timedelta(seconds=i * random.uniform(0.2, 1.2))).isoformat()

        test_message = {
            "data_id": f"test-{i:03d}",
            "mmsi": mmsi,
            "timestamp": ts,
            "lon": float(lon),
            "lat": float(lat),
            "sog": float(round(sog, 2)),
            "cog": float(round(cog, 1)),
            "heading": int(heading),
        }

        data = msgpack.packb(test_message, use_bin_type=True)
        await nc.publish(topic, data)

        # Optionnel: flush périodique plutôt qu'à chaque msg (un peu plus efficace)
        if (i + 1) % 10 == 0:
            await nc.flush()

        print(
            f"[{i + 1:02d}/{total_messages}] "
            f"mmsi={mmsi} lat={test_message['lat']:.5f} lon={test_message['lon']:.5f} "
            f"sog={test_message['sog']} cog={test_message['cog']} heading={test_message['heading']}"
        )

        if delay_s > 0:
            await asyncio.sleep(delay_s)

    # Flush final
    await nc.flush()

    print("\n✓ Série publiée !")
    print("Vérifiez les logs du forger et de l'enricher...")

    await nc.close()


if __name__ == "__main__":
    asyncio.run(
        publish_test_messages(
            nats_url="nats://localhost:4222",
            topic="nova.parsed",
            total_messages=50,
            mmsi_count=10,
            delay_s=0.05,
            seed=42,
        )
    )
