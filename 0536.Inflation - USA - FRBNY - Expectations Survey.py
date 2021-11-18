#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[2]:


#Descargo desde el link
url = 'https://www.newyorkfed.org/medialibrary/interactives/sce/sce/downloads/data/frbny-sce-data.xlsx'
#Guardo el contenido en una variable así lo puedo consultar varias veces
archivo = requests.get(url)


# In[3]:


#Guardo los nombres de las hojas en una lista
sheet_list = pd.ExcelFile(archivo.content).sheet_names


# In[4]:


#Mantengo las que tienen expectations o expectatations
#Como hay casos que tienen mayuscula, no se utiliza Expect
final_list = [termino for termino in sheet_list if "xpect" in termino]

#Elimino las que tienen Demo
final_list = [termino for termino in final_list if "Demo" not in termino]
#Elimino las que tienen distr
final_list = [termino for termino in final_list if "distr" not in termino]


# In[5]:


for indice, sheet in enumerate(final_list):
    dframe = pd.read_excel(archivo.content, sheet_name = sheet, skiprows = 3)
    #Renombro la columna
    dframe.rename(columns={dframe.columns[0]:'Date'}, inplace=True)
    
    #Cambio a formato fecha
    dframe['Date'] = pd.to_datetime(dframe['Date'].astype(str).str[:4] + '-' + dframe['Date'].astype(str).str[4:], 
                             format='%Y-%m').dt.strftime('%Y-%m-%d')
    dframe.set_index('Date', inplace=True)
    
    #creo un prefijo que excluya el termino expectation
    prefijo = sheet.replace(' expectatations','').replace(' expectations','').replace(' Expectations','').replace(' expectation','')
    
    #Agrego el nombre de la hoja
    dframe = dframe.add_prefix(prefijo + ' - ')
    
    if indice == 0:
        df = dframe
    else:
        df = df.merge(dframe, left_index=True, right_index=True, how='outer')


# In[6]:


#Excluyo las columnas del dataframe final que contenga el termino point
exclude_columns = [termino for termino in df.columns if "point" in termino]

df.drop(exclude_columns, axis=1, inplace=True)

df['country'] = 'USA'

alphacast.datasets.dataset(536).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

