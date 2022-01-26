from comet_utils.database.adatabase import ADatabase
import pandas as pd

class CometHistorian(ADatabase):
    
    def __init__(self,mongouser,mongokey):
        super().__init__("comet_historian",mongouser,mongokey)
    
    def get_symbols(self,api):
        try:
            db = self.client[self.name]
            table = db[f"{api}_prices"]
            data = table.distinct("crypto")
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.name,"fills",str(e))