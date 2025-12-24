import msgpack
import asyncio
import functools
import nats
import msgpack
from logging import basicConfig, getLogger, Logger
from rich.console import Console
from rich.logging import RichHandler

basicConfig(
    level='DEBUG',  # niveau de log a changer avant mise en production
    format="%(message)s",
    datefmt="[%Y-%m-%dT%H:%M:%S]",
    handlers=[RichHandler(console=Console(stderr=True))],
)

_LOGGER: Logger = getLogger('publisher')
VERSION = '4.0.0'

def nats_publish_decorator(topic: str, ip: str, port: int):
    """
    Décorateur pour publier le résultat d'une fonction sur un topic NATS.

    Args:
        topic (str): Le topic NATS sur lequel publier le message.
        ip (str): L'adresse IP du serveur NATS.
        port (int): Le port du serveur NATS.

    Returns:
        callable: Le décorateur à appliquer à la fonction.
    """
    def decorator(function):
        if asyncio.iscoroutinefunction(function):  # verification si la fonction est asynchrone
            
            @functools.wraps(function)  # permet de conserver les métadonnées de la fonction originale
            async def wrapper(*args, **kwargs):
                result = await function(*args, **kwargs)  # appel de la fonction originale
                await _publish_message(result, topic, ip, port)  # envoie du resultat de la fonction à NATS
                return result
            return wrapper
            
        else:  # Si la fonction est synchrone
            @functools.wraps(function)
            def wrapper(*args, **kwargs):
                result = function(*args, **kwargs)  # appel de la fonction originale
                asyncio.run(_publish_message(result, topic, ip, port))  # envoie du resultat de la fonction à NATS
                return result
            return wrapper
    return decorator


async def _publish_message(message, topic: str, ip: str, port: int) -> None:
    """
    Fonction interne pour publier un message sur NATS.

    Args:
        message: Le message à publier (sera sérialisé en MessagePack).
        topic (str): Le topic NATS.
        ip (str): L'adresse IP du serveur NATS.
        port (int): Le port du serveur NATS.
    """
    _LOGGER.debug("Connexion to the NATS server at {}:{}".format(ip, port))
    nc = await nats.connect(f"nats://{ip}:{port}")  # connexion au serveur NATS

    message: bytes = msgpack.packb(message)  # type: ignore  packb renvoie toujours des bytes

    _LOGGER.debug("Publishing message {} to topic: '{}'".format(message, topic))
    await nc.publish(topic, message)  # publication du message en MessagePack
    await nc.flush()
