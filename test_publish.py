"""
Script de test pour publier un message AIS parsé sur NATS.

Ce script simule la sortie du Parser en publiant un message
ParsedAISMessage sur le topic nova.parsed.
"""
import asyncio
from datetime import datetime

import msgpack
import nats


async def publish_test_message():
    """Publie un message AIS de test sur nova.parsed."""

    # Connexion à NATS
    print("Connexion à NATS...")
    nc = await nats.connect("nats://localhost:4222")
    print("✓ Connecté à NATS")

    # Créer un message AIS de test
    test_message = {
        "data_id": "test-001",
        "mmsi": 123456789,
        "timestamp": datetime.now().isoformat(),
        "lon": -5.5,
        "lat": 48.2,
        "sog": 10.5,
        "cog": 90.0,
        "heading": 92,
    }

    print(f"\nPublication du message test sur nova.parsed:")
    print(f"  MMSI: {test_message['mmsi']}")
    print(f"  Position: {test_message['lat']}, {test_message['lon']}")
    print(f"  SOG: {test_message['sog']} kt")
    print(f"  COG: {test_message['cog']}°")

    # Sérialiser en MessagePack
    data = msgpack.packb(test_message, use_bin_type=True)

    # Publier sur nova.parsed
    await nc.publish("nova.parsed", data)
    await nc.flush()

    print("\n✓ Message publié !")
    print("\nVérifiez les logs du forger et de l'enricher...")

    # Fermer la connexion
    await nc.close()


if __name__ == "__main__":
    asyncio.run(publish_test_message())
