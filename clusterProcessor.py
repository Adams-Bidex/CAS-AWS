import numpy as np
import pandas as pd

df = pd.read_csv('workers - Divisions.csv')
df.columns
cluster=[*df["Clusters"].unique()]
level=[*df["Level"].unique()]
location=[*df["Location"].unique()]

data=[]
loca=[]
leve=[]
clusterr=[]
min=[]

for loc in location:
   for clust in cluster:
       for lev in level:
           result=loc+clust+lev
           if(lev=="L1"):
               number=np.random.uniform(35000,45000)
               min_val=number
               loca.append(loc)
               leve.append(lev)
               clusterr.append(clust)
               min.append(min_val)
               #mid.append(mid_val)
               #max.append(max_val)
           elif(lev=="L2"):
               number=np.random.uniform(50000,60000)
               min_val=number
               loca.append(loc)
               leve.append(lev)
               clusterr.append(clust)
               min.append(min_val)
               #mid.append(mid_val)
               #max.append(max_val)
           elif(lev=="L3"):
               number=np.random.uniform(65000,75000)
               min_val=number
               loca.append(loc)
               leve.append(lev)
               clusterr.append(clust)
               min.append(min_val)
               #mid.append(mid_val)
               #max.append(max_val)
           elif(lev=="L4"):
               number=np.random.uniform(80000,90000)
               min_val=number
               loca.append(loc)
               leve.append(lev)
               clusterr.append(clust)
               min.append(min_val)
               #mid.append(mid_val)
               #max.append(max_val)
           elif(lev=="L5"):
               number=np.random.uniform(110000,130000)
               min_val=number
               loca.append(loc)
               leve.append(lev)
               clusterr.append(clust)
               min.append(min_val)
               #mid.append(mid_val)
               #max.append(max_val)
           elif(lev=="L6"):
               number=np.random.uniform(140000,170000)
               min_val=number
               loca.append(loc)
               leve.append(lev)
               clusterr.append(clust)
               min.append(min_val)
               #mid.append(mid_val)
               #max.append(max_val)
           elif(lev=="L7"):
               number=np.random.uniform(170000,200000)
               min_val=number
               loca.append(loc)
               leve.append(lev)
               clusterr.append(clust)
               min.append(min_val)
               #mid.append(mid_val)
               #max.append(max_val)
           data.append(result)
data
min

dff=pd.DataFrame(list(zip(loca,leve,clusterr,data,min)
                      ), columns=["location","level","Cluster","Concate","min"]) 
dff["mid"]=(dff["min"]*1.15).round(0)
dff["max"]=(dff["min"]*1.3).round(0) 
dff["min"]=dff["min"].round(0)
to_round=["min","mid","max"]

for col in to_round:
    dff[col]=(dff[col]/100).round()*100

dff.to_csv("ClusterAll.csv")  