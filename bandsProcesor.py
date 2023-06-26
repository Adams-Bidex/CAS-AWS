import pandas as pd 
def bandsProcessor():
    #Location,Title,Level,Family,Job_Code,Twenty_Fifth,Fifty,Seventy_Fifth,Max
    df= pd.read_csv("datasets/workers2.csv")
    # List of columns to round
    cols_to_round = ['Twenty_Fifth', 'Fifty', 'Seventy_Fifth', 'Max']
    # Round each column in cols_to_round to the nearest 100
    for col in cols_to_round:
        df[col] = (df[col] / 100).round() * 100
    df=df.to_dict('records')
    return {'data':df}