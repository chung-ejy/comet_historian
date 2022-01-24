from comet_utils.database.adatabase import ADatabase
import pandas as pd

class CometHIstorian(ADatabase):
    
    def __init__(self,mongouser,mongokey):
        super().__init__("comet_historian",mongouser,mongokey)