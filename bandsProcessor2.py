import pandas as pd
import numpy as np

def bandsProcessorNew():
    
    df= pd.read_csv("workers - ClusterAlln.csv")
    df=df.to_dict('records')
    return {'data':df}

