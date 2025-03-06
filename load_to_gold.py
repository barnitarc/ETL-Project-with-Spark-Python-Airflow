import pandas as pd
from datetime import datetime
def move_to_gold():
    df=pd.read_parquet('/home/barnita/work/airflow-projects/dags/project-3/data/silver/',engine='fastparquet')
    folder=datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    filename='gold_output'+str(folder)+'.csv'
    path="/home/barnita/work/airflow-projects/dags/project-3/data/gold/"+str(filename)
    df['start_date']=df['start_date'].dt.tz_localize('Asia/Kolkata')
    df['end_date']=df['end_date'].dt.tz_localize('Asia/Kolkata')
    df.to_csv(path,sep='|',header=True)
   
move_to_gold()
