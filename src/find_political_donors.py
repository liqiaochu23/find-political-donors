# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import numpy as np
import pandas as pd
import os.path
import os
basepath=os.path.dirname(__file__)
#filepath=os.path.abspath(os.path.join(basepath,".."))
filepath=os.path.dirname(basepath)

#print(os.path.dirname(os.path.abspath(__file__)))
# import numpy and pandas library 


filename=os.path.join(filepath,'input','itcont.txt').replace("\\","/")  ##import input file, copy the directory replace the current one
data = pd.read_csv(filename, sep="|", header=None)      ##call pd.read_csv to store imput in data

output_1=os.path.join(filepath,'output','medianvals_by_zip.txt').replace("\\","/")
output_2=os.path.join(filepath,'output','medianvals_by_date.txt').replace("\\","/")

df=data.iloc[:,[0,10,13,14,15]]                         ##selecct column where we would like to analysis and store them in dataframe df
df.columns=['CMTE_ID','ZIP_CODE','TRANSACTION_DT','TRANSACTION_AMT','OTHER_ID']     ##rename each column for output purpose


ZIP_CODE=df.loc[:,'ZIP_CODE']                           ##start cleaning data by changing ZIP_CODE into the same format 
new_ZIP_CODE=[]
for items in ZIP_CODE:
    if len(str(items))==9:
       new_ZIP_CODE.append(int(str(items)[:5]))
    elif len(str(items))==8:
       new_ZIP_CODE.append(int(str(items)[:4]))
    else:
       new_ZIP_CODE.append(items)
new_TRANSACTION_DT=[]
new_ZIP_CODE=([str(item).zfill(5) for item in new_ZIP_CODE])


TRANSACTION_DT=df.loc[:,'TRANSACTION_DT']               
new_TRANSACTION_DT=([str(item).zfill(8) for item in TRANSACTION_DT])
df['TRANSACTION_DT']=new_TRANSACTION_DT
df['ZIP_CODE']=new_ZIP_CODE
OTHER_ID=df.loc[:,'OTHER_ID']


df=df[pd.isnull(df['OTHER_ID'])].reset_index(drop=True)     ##create two dataframes, df_1 for output 1 and df_2 for output 2
df_1=df.drop(['OTHER_ID','TRANSACTION_DT'],axis=1)          ##drop all missing values in df_1
df_1['NUM_ID']=1                                            ##set initial value for num_id equals to 1
df_1['MEDIANVALS_ZIP']=df['TRANSACTION_AMT']


df_2=df.drop(['OTHER_ID','ZIP_CODE'],axis=1)
df_2['NUM_ID']=1
df_2['MEDIANVALS_DATE']=df['TRANSACTION_AMT']





for i,row_1 in df_1.iterrows():     #loop through each row in df_1 and apply function to some columns count sum and median
   
    new_df=pd.DataFrame(index=list(range(0,i+1)),columns=['CMTE_ID','ZIP_CODE','TRANSACTION_AMT','NUM_ID','MEDIANVALS_ZIP','index1']) 
    
    

    new_df.iloc[0:(i+1),:]=df_1.iloc[0:(i+1),:] ##load data from df_1 to new_df row by row
    new_df['index1']=list(range(0,i+1))			##add each input row a reference number 
    new_df[['ZIP_CODE','TRANSACTION_AMT','NUM_ID','MEDIANVALS_ZIP','index1']] = new_df[['ZIP_CODE','TRANSACTION_AMT','NUM_ID','MEDIANVALS_ZIP','index1']].apply(pd.to_numeric)
    
    if i<1:
        x=new_df.iloc[-1,-1]
        ZIP_CODE_x=new_df.iloc[-1,1]
    else:
        x=final_df_5.iloc[-1,-1]
        ZIP_CODE_x=final_df_5.iloc[-1,1]

    
    f={'TRANSACTION_AMT':['sum'],'NUM_ID':['sum'],'MEDIANVALS_ZIP':['median'],'index1':lambda x : x.astype(int).sum()} ##create function dictionary for groupby aggregation     
    final_df_5=new_df.groupby(['CMTE_ID','ZIP_CODE']).agg(f).reset_index()
    final_df_5=final_df_5.sort_values([('index1','<lambda>')])
   
     
    final_df_5.MEDIANVALS_ZIP=final_df_5.MEDIANVALS_ZIP.round(decimals=0).astype(np.int64)  #round MEDIANVALS to integer as required

    final_df_5=final_df_5[['CMTE_ID','ZIP_CODE','MEDIANVALS_ZIP','NUM_ID','TRANSACTION_AMT','index1']]
    new_ZIP_CODE_1=[]
    ZIP_CODE_1=final_df_5.loc[:,'ZIP_CODE']
    new_ZIP_CODE_1=([str(item).zfill(5) for item in ZIP_CODE_1])
    
    final_df_5['ZIP_CODE']=new_ZIP_CODE_1
    
    y=final_df_5.iloc[-1,-1]
    ZIP_CODE_y=final_df_5.iloc[-1,1]

       
    if np.logical_and(x==y,ZIP_CODE_x==ZIP_CODE_y)==True :  ##chech the last row of final_df_5, if it equals to last iteration then go to the second last
        if i<1:
            pd.DataFrame(final_df_5.iloc[-1,0:5]).T.to_csv(output_1,index=False,header=False,mode='a',sep='|',encoding='utf-8')## output medianvals_by_zip
        else:
            for j in range(len(final_df_5.loc[:,'NUM_ID'])-1,-1,-1):
                if final_df_5.iloc[j,3]==1:
                    pd.DataFrame(final_df_5.iloc[j,0:5]).T.to_csv(output_1,index=False,header=False,mode='a',sep='|',encoding='utf-8')## output medianvals_by_zip
                    break
                else:
                    j=j+1
    else:
        pd.DataFrame(final_df_5.iloc[-1,0:5]).T.to_csv(output_1,index=False,header=False,mode='a',sep='|',encoding='utf-8')## output medianvals_by_zip

    





df_3=df_2.groupby(['CMTE_ID','TRANSACTION_DT'])[['NUM_ID','TRANSACTION_AMT']].sum() ## calculate NUM_ID and TRANSACTION_AMT by grouping the CMTE_ID and TRANSACTION_DT
df_3=df_3.reset_index()

df_4=df_2.groupby(['CMTE_ID','TRANSACTION_DT'])[['MEDIANVALS_DATE']].median()
df_4=df_4.reset_index()
df_4.loc[:,'MEDIANVALS_DATE']=np.round(df_4.loc[:,'MEDIANVALS_DATE'],decimals=0).astype(np.int64)

df_3['MEDIANVALS_DATE']=df_4.loc[:,'MEDIANVALS_DATE']
df_3=df_3[['CMTE_ID','TRANSACTION_DT','MEDIANVALS_DATE','NUM_ID','TRANSACTION_AMT']]

df_3.to_csv(output_2,sep='|',index=False,header=False) ## output medianvals_by_date
