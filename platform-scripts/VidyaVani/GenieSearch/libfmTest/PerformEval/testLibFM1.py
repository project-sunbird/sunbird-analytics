
import os 
import subprocess
import numpy as np 
import pandas as pd
import random 

#sparsity=[1,10,100,1000]
sparsity=[1,10, 100, 1000]
dev_num=100000
content_num=1000000
#matrix creation
for i in sparsity:
    row_count=dev_num*i
    dev_content=np.zeros(((row_count),2),dtype=int)
    ranking=np.zeros(((row_count),1))
    j=0
    dev=1
    while (j<row_count):
        dev_content[range(j,(j+i)),:]= np.column_stack((np.full((i,1),dev),np.random.choice(content_num, i)))  
        ranking[range(j,(j+i)),:]=np.random.rand(i,1)
        dev=dev+1
        j=(j+i)
        
    train=np.concatenate((dev_content,ranking),axis=1)
    test=np.concatenate((dev_content[[1,2],:][:],np.zeros((2,1))),axis=1)
    #test=np.concatenate((dev_content,np.zeros(((dev_num*i),1))),axis=1)
    #save train to csv
    dataframe = pd.DataFrame(data=train.astype(float))
    dataframe.to_csv('train.csv', sep=' ', header=False, float_format='%.2f', index=False)
    #save test to csv
    dataframe = pd.DataFrame(data=test.astype(float))
    dataframe.to_csv('test.csv', sep=' ', header=False, float_format='%.2f', index=False)
   
    call="./libfmPerfEval1.sh "+str(i)
    subprocess.call([call], shell=True)

