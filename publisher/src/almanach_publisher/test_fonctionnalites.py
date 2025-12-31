import json
from almanach_publisher.publisher import nats_publish_decorator

@nats_publish_decorator(topic="ais.results", ip="127.0.0.1", port=4222)
def traiter_trame_ais(trame):
    # Chargement de la trame AIS au format JSON
    result = json.loads(trame)
    if not isinstance(result, dict):
        raise SystemExit("json must be parsed to a JSON object")
    
    # Logique de traitement de la trame AIS ici
    
    return result

if __name__ == "__main__":
    trame : str = '{"uuid":"1","mmsi":123456789,"latitude":48.8566,"longitude":2.3522}'
    traiter_trame_ais(trame)
