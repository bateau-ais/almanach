# https://opendata.stackexchange.com/questions/15329/free-source-of-ais-data-api

from pandas.core.series import Series
from datetime import datetime
import pandas as pd


class AIS_data:
    # Informations standards AIS
    mmsi : int # numero d'identification du navire
    status : str  # statut du navire
    cog : float  # cap en degres, direction (par rapport au nord géographique) (course over ground) (compris entre 0 et 359) (provient du GPS)
    sog : float  # vitesse sur le fond en noeuds (speed over ground) (compris entre 0 et 102.2 noeuds) (provient du GPS)
    rateofturn : float  # taux de rotation en degres par minute (compris entre 0 et 720)
    longitude : float  # longitude en degres
    latitude : float  # latitude en degres
    heading : int  # cap vrai, direction vers laquelle pointe le bateau (pas forcément sa route) (par rapport au nord magnétique) (compris entre 0 et 359)
    bearing : float  # gisement en degres (direction d'un point par rapport au navire) (compris entre 0 et 359)
    time : datetime  # date/heure de la trame AIS (UTC) (dans les trames brutes, seule la seconde est transmise, le reste est à reconstituer)

    # Informations complémentaires AIS
    imo : int | None # numero d'identification international du navire
    callsign : str | None # indicatif d'appel du navire
    name : str | None # nom du navire
    vesseltype : int | None # type de navire selon la classification AIS
    beam : float | None # largeur maximale du navire en metres
    draft : float | None  # ou draught, tirant d'eau du navire en metres
    destination : str | None  # destination du navire
    eta : str | None  # heure estimée d'arrivée au port de destination (UTC month/date hour:minute)
    trackstarttime : datetime | None  # date/heure de debut du trajet
    trackendtime : datetime | None  # date/heure de fin du trajet
    length : float | None # longueur du navire en metres
    width : float | None  # largeur du navire en metres
    durationminutes : int | None  # duree du trajet en minutes
    vesselgroup : str | None  # groupe de navires selon la classification AIS
    cargo : str | None  # type de cargaison

    def __init__(self) -> None:
        self.mmsi = 0
        self.status = ""
        self.cog = 0.0
        self.sog = 0.0
        self.rateofturn = 0.0
        self.longitude = 0.0
        self.latitude = 0.0
        self.heading = 0
        self.bearing = 0.0
        self.time = datetime.now()

        self.imo = None
        self.callsign = None
        self.name = None
        self.vesseltype = None
        self.beam = None
        self.draft = None
        self.destination = None
        self.eta = None
        self.trackstarttime = None
        self.trackendtime = None
        self.length = None
        self.width = None
        self.durationminutes = None
        self.vesselgroup = None
        self.cargo = None

        self.error = False  # indique si une erreur a été détectée lors de la vérification des valeurs
    
    def parse_from_csv_raw(self, raw_frame: Series):
        """
        Parse une trame AIS et remplit les attributs de l'objet AIS_data.
        """
        self.mmsi = int(raw_frame['mmsi'])
        self.status = str(raw_frame['navstatus'])
        self.cog = float(raw_frame['cog'])        
        self.sog = float(raw_frame['sog'])
        
        # rateofturn not present in CSV

        self.longitude = float(raw_frame['longitude'])
        self.latitude = float(raw_frame['latitude'])
        self.heading = int(raw_frame['heading'])

        # bearing not present

        self.time = datetime.fromtimestamp(int(raw_frame['timeoffix']))  # timeoffix is in seconds since epoch. Convert to datetime (e.g. 1633072800 -> 2021-10-01 00:00:00 UTC)

        if not pd.isna(raw_frame['imonumber']):
            self.imo = int(float(raw_frame['imonumber']))

        if not pd.isna(raw_frame['callsign']):
            self.callsign = raw_frame['callsign']

        if not pd.isna(raw_frame['name']):
            self.name = raw_frame['name']

        if not pd.isna(raw_frame['vesseltype']):
            if '-' in str(raw_frame['vesseltype']):
                self.vesseltype = int(str(raw_frame['vesseltype']).split('-')[0])

        # draft not present
        # destination not present
        # eta not present
        # trackstarttime not present
        # trackendtime not present
        
        if not pd.isna(raw_frame['length']):
            self.length = float(raw_frame['length'])
        
        if not pd.isna(raw_frame['beam']):
            self.width = float(raw_frame['beam'])  # assuming beam is width

        # durationminutes not present
        # vesselgroup not present

        if not pd.isna(raw_frame['cargo']):
            self.cargo = raw_frame['cargo']

        try:
            self.check_values()  # TODO: gestion des erreurs si valeurs hors plage pour passer à la trame suivante sans bloquage
        except ValueError as e:
            self.error = True  # une erreur dans les valeurs
        
        return self
    
    def check_values(self) -> None:
        """
        Vérifie que les valeurs des attributs sont dans les plages attendues.
        Lève une exception si une valeur est hors plage.
        """
        if not (0 <= self.cog <= 359):
            raise ValueError(f"COG {self.cog} is not between 0 and 359")
        
        if not (0 <= self.sog <= 102.2):
            raise ValueError(f"SOG {self.sog} is not between 0 and 102.2")
        
        if not (-180 <= self.longitude <= 180):
            raise ValueError(f"Longitude {self.longitude} is not between -180 and 180")

        if not (-90 <= self.latitude <= 90):
            raise ValueError(f"Latitude {self.latitude} is not between -90 and 90")

        if not (0 <= self.heading <= 359):
            raise ValueError(f"Heading {self.heading} is not between 0 and 359")
