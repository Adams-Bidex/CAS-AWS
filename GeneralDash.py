import  os
import pandas as pd
from rich import pretty
import sqlalchemy as sql
pretty.install()
async def DashWrangler(Division,Level,Location):
    ##create engine that will read the database
    Engine= sql.create_engine("sqlite:///Database/Population.sqlite")

    ## Engine is now the class of sqlite database
    ## connect to the created database
    connection=Engine.connect()

    ## read data from table in database
    PopulationData=pd.read_sql_query("SELECT * FROM Population",
                                     con=connection)
    #PopulationData.columns
    connection.close()

    PopulationFields=['_id','Legal_Name', 'Cont_status','Hire_Date','Country', 'Location', 'Division','Company', 
       'Business_Title','Gender','Curr_Level','Annual_Sal','Currency', 'FTE']
    Employee_Pop= PopulationData[PopulationFields]
    Employee_dict=Employee_Pop.to_dict('records')
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
    df['Hire_Date']=pd.to_datetime(df['Hire_Date'], format='mixed')
    df['Year']= df['Hire_Date'].dt.year
    F_Pop=df[df['Gender'].isin(['Female'])]
    M_Pop=df[df['Gender'].isin(['Male'])]
    F_Prop=[len(F_Pop)/(len(F_Pop)+len(M_Pop))]
    Females=[len(F_Pop)]
    Males=[len(M_Pop)]
    F_Med=F_Pop['Annual_Sal'].median()
    M_Med=M_Pop['Annual_Sal'].median()
    PayGap=[0]
    if(F_Med/M_Med>0):
        PayGap=[round(F_Med/M_Med,3)]
    else:
        PayGap=[0]
    
    SalaryByDivisionSummary=df[['Division','Annual_Sal']] \
                     .groupby(['Division'])  \
                     .sum() \
                     .fillna(0) \
                     .reset_index()  \
                     .sort_values('Annual_Sal')
    
    MinSal=[SalaryByDivisionSummary['Annual_Sal'].min()]
    MaxSal=[SalaryByDivisionSummary['Annual_Sal'].max()]
    MedSal=[df['Annual_Sal'].median()]
    AvgSal=[df['Annual_Sal'].mean()]
    Summaries=pd.DataFrame(list(zip(TotalEmployees,
                                    TotalCurrentSalary,
                                    PayGap,F_Prop,MedSal,AvgSal,Females,Males)),
                                    columns=['Headcounts',
                                             'TotCurrentSal',
                                             'PayGap',
                                             'F_Prop','MedSal','AvgSal','Females','Males'])                 
    CountByLevel= df[['Curr_Level']] \
                     .groupby(['Curr_Level'])  \
                     .value_counts() \
                     .fillna(0) \
                     .reset_index()  \
                     .sort_values('count')
    
    SalaryByYearSummary=df[['Hire_Date','Annual_Sal']] \
                     .set_index('Hire_Date') \
                     .resample(rule="Y") \
                     .sum() \
                     .fillna(0) \
                     .reset_index()\
                     .sort_values(by='Annual_Sal')  
    SalaryByYearSummary['Hire_Date']=SalaryByYearSummary['Hire_Date'].dt.year
    
    MedSalaryByYearGender=df[['Year','Gender','Annual_Sal']] \
                     .groupby(['Year','Gender']) \
                     .median() \
                     .fillna(0) \
                     .reset_index()
    div2 = MedSalaryByYearGender['Gender'].unique()
    list2 = sorted(list(div2))

    MedSalaryByYearGender=MedSalaryByYearGender \
                     .pivot(index=['Year'], columns=['Gender'], values=['Annual_Sal']) \
                     .fillna(0) \
                     .reset_index() \
                     .set_axis(['Year', *list2], axis=1)\
                     .sort_values(by='Year')  
    
    MedSalaryByGenderLocation=df[['Location','Gender','Annual_Sal']] \
                     .groupby(['Location','Gender']) \
                     .median() \
                     .fillna(0) \
                     .reset_index()
    div = MedSalaryByGenderLocation['Gender'].unique()
    list1 = sorted(list(div))

    MedSalaryByGenderLocation=MedSalaryByGenderLocation \
                     .pivot(index=['Location'], columns=['Gender'], values=['Annual_Sal']) \
                     .fillna(0) \
                     .reset_index() \
                     .set_axis(['Location', *list1], axis=1)\
                     .sort_values(by='Female')

    def GapEstimator(x):
        if (x['Female'] > 0 and x['Male'] > 0):
            return x['Female'] / x['Male']
        elif (x['Female'] == 0 or x['Male'] == 0):
            return 0
        else:
            return 0

    MedSalaryByGenderLocation['PayGap'] = MedSalaryByGenderLocation.apply(GapEstimator,axis=1)
    MedSalaryByGenderLocation['PayGap'] = (MedSalaryByGenderLocation['PayGap']*100).round(2)

    MedSalaryByGenderLocation=MedSalaryByGenderLocation.sort_values(by="PayGap", ascending=False)
    
    MedSalaryByGenderLocation=MedSalaryByGenderLocation.to_dict('records') 
    MedSalaryByYearGender=MedSalaryByYearGender.to_dict('records')
    SalaryByDivisionSummary= SalaryByDivisionSummary.to_dict('records')
    Summaries=Summaries.to_dict('records')
    CountByLevel=CountByLevel.to_dict('records')
    SalaryByYearSummary=SalaryByYearSummary.to_dict('records')
    Pop=df.to_dict('records')
    return {'df':Employee_dict,'Pop':Pop,'Summaries':Summaries,
            'SalaryByDivisionSummary':SalaryByDivisionSummary,
            'CountByLevel':CountByLevel, 
            'SalaryByYearSummary':SalaryByYearSummary, 
            'MedSalaryByYearGender':MedSalaryByYearGender,
            'MedSalaryByGenderLocation':MedSalaryByGenderLocation}