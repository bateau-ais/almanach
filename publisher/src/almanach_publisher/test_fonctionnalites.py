from almanach_publisher.publisher import nats_publish_decorator

trame = "Ceci est une trame AIS exemple"

@nats_publish_decorator(topic="ais.results", ip="127.0.0.1", port=4222)
def traiter_trame_ais(trame):
    # Logique de traitement
    result: str = "Trame traitée: {}".format(trame)
    return result

if __name__ == "__main__":
    traiter_trame_ais(trame)
