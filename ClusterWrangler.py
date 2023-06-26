import  os
import pandas as pd
from rich import pretty
import sqlalchemy as sql
pretty.install()
async def ClusterBandsWrangler(Division,Level,Location):
    ##create engine that will read the database
    Engine= sql.create_engine("sqlite:///Database/MarketData.sqlite")

    ## Engine is now the class of sqlite database
    ## connect to the created database
    con1=Engine.connect()

    ## read data from table in database
    MarketData=pd.read_sql_query("SELECT * FROM MarketData",
                                     con=con1)
    con1.close()
    ##create engine that will read the database
    Engine2= sql.create_engine("sqlite:///Database/Clusters.sqlite")

    ## Engine is now the class of sqlite database
    ## connect to the created database
    con2=Engine2.connect()

    ## read data from table in database
    ClustersData=pd.read_sql_query("SELECT * FROM Clusters",
                                     con=con2)
    con2.close()
    ##create engine that will read the database
    Engine3= sql.create_engine("sqlite:///Database/Population.sqlite")

    ## Engine is now the class of sqlite database
    ## connect to the created database
    con3=Engine3.connect()
  
    ## read data from table in database
    PopulationData=pd.read_sql_query("SELECT * FROM Population",
                                     con=con3)
    con3.close()

    PopulationFields=['_id','Legal_Name', 'Cont_status','Hire_Date','Country', 'Location', 'Division','Company', 
       'Business_Title','Gender','Curr_Level','Annual_Sal','Currency', 'FTE']
    Employee_Pop= PopulationData[PopulationFields]
    ClustersFields=['_id','Cluster']
    ClusterInfo= ClustersData[ClustersFields]
    MarketData.columns
    MarketDataFields= ['location', 'level', 'Cluster', 'Code', 'Concate', 'min','mid', 'max']
    MarketInfo=MarketData[MarketDataFields]
    ClusterMapper=Employee_Pop.merge(ClusterInfo,on='_id',how='left')

    ClusterMapper['Concate']= ClusterMapper['Location']+ClusterMapper['Cluster']+ClusterMapper['Curr_Level']

    Employee_MarketBands= ClusterMapper.merge(MarketInfo[['Concate','min','mid', 'max']], on='Concate',how='left')
    Employee_MarketBands['min']=Employee_MarketBands['min']*Employee_MarketBands['FTE']
    Employee_MarketBands['mid']=Employee_MarketBands['mid']*Employee_MarketBands['FTE']
    Employee_MarketBands['max']=Employee_MarketBands['max']*Employee_MarketBands['FTE']
    Employee_MarketBands['Variation']=Employee_MarketBands['Annual_Sal']-Employee_MarketBands['min']
    Employee_MarketBands['Var_Percent']=(Employee_MarketBands['Variation']/Employee_MarketBands['Annual_Sal']).round(3)
    def CatchUpCalculator(x):
        if(x['Variation']<0):
            return x['Variation']* -1
        else:
            return 0
    
    Employee_MarketBands['Catch_up']= Employee_MarketBands.apply(CatchUpCalculator,axis=1)
    Employee_dict=Employee_MarketBands.to_dict('records')
    df=Employee_dict
    df=pd.DataFrame(df)
    if(len(Division)>0):
        df=df[df['Division'].isin([*Division])]
    if(len(Level)>0):
        df=df[df['Curr_Level'].isin([*Level])]
    if(len(Location)>0):
        df=df[df['Location'].isin([*Location])]        
    #UniqueId=[df['_id'].unique()]
    TotalEmployees=[len(df)]
    TotalCurrentSalary=[df['Annual_Sal'].sum()]
    TotalMarketSalary=[df['mid'].sum()]
    TotalCatchup=[df['Catch_up'].sum()]
    AvgPercent=[round(df['Var_Percent'].mean(),3)]
    df['Hire_Date']=pd.to_datetime(df['Hire_Date'], format='mixed')
    df['Year']= df['Hire_Date'].dt.year
    F_Pop=df[df['Gender'].isin(['Female'])]
    M_Pop=df[df['Gender'].isin(['Male'])]
    F_Prop=[len(F_Pop)/(len(F_Pop)+len(M_Pop))]
    F_Med=F_Pop['Annual_Sal'].median()
    M_Med=M_Pop['Annual_Sal'].median()
    PayGap=[0]
    if(F_Med/M_Med>0):
        PayGap=[round(F_Med/M_Med,3)]
    else:
        PayGap=[0]
    
    SalaryByDivisionSummary=df[['Division','Annual_Sal','Catch_up']] \
                     .groupby(['Division'])  \
                     .sum() \
                     .fillna(0) \
                     .reset_index()  \
                     .sort_values('Catch_up')
    
    MinSal=[SalaryByDivisionSummary['Catch_up'].min()]
    MaxSal=[SalaryByDivisionSummary['Catch_up'].max()]
    
    Summaries=pd.DataFrame(list(zip(TotalEmployees,
                                    TotalCurrentSalary,
                                    TotalMarketSalary,
                                    TotalCatchup, AvgPercent,MinSal,MaxSal,PayGap,F_Prop)),
                                    columns=['Headcounts','TotCurrentSal','TotalMarketSal',
                                    'TotCatchUp','AvgPercent','MinSal','MaxSal','PayGap','F_Prop'])                 
    CountByDivision= df[['Division']] \
                     .groupby(['Division'])  \
                     .value_counts() \
                     .fillna(0) \
                     .reset_index()  \
                     .sort_values('count')
    
    SalaryByYearSummary=df[['Year','Catch_up']] \
                     .groupby(['Year'])  \
                     .sum() \
                     .fillna(0) \
                     .reset_index()  \
                     .sort_values('Year')           
    
    SalaryByDivisionSummary= SalaryByDivisionSummary.to_dict('records')
    Summaries=Summaries.to_dict('records')
    CountByDivision=CountByDivision.to_dict('records')
    SalaryByYearSummary=SalaryByYearSummary.to_dict('records')
    Pop=df.to_dict('records')
    return {'df':Employee_dict,'Pop':Pop,'Summaries':Summaries,
            'SalaryByDivisionSummary':SalaryByDivisionSummary,
            'CountByDivision':CountByDivision, 'Catch_upByYearSummary':SalaryByYearSummary }

