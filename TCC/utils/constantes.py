import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import datetime
from dotenv import load_dotenv
from statsmodels.tsa.stattools import adfuller, grangercausalitytests
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import warnings
import os

load_dotenv('env/.env')

API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv('API_SECRET')

PATH_TABELA_CONSOLIDADA = r"TCC/data/tabela_consolidada.csv"

DATA_INICIO = '2016-12-31' # Inicio era varejo
DATA_DIVISAO_ERA = '2020-03-11' # pandemia no Brasil / inicio era institucional
DATA_FIM = '2025-08-01' # Fim dos dados disponiveis

# Dataframe da era varejo
date_range_era_varejo = pd.date_range(start=DATA_INICIO, end=DATA_DIVISAO_ERA, freq='D')
DF_ERA_VAREJO = pd.DataFrame(date_range_era_varejo, columns=['Data_UTC'])
DF_ERA_VAREJO['era'] = 'varejo'

# Dataframe da institucional
date_range_era_institucional = pd.date_range(start=DATA_DIVISAO_ERA, end=DATA_FIM, freq='D')
DF_ERA_INSTITUCIONAL = pd.DataFrame(date_range_era_institucional, columns=['Data_UTC'])
DF_ERA_INSTITUCIONAL['era'] = 'institucional'

df_periodo = (pd.DataFrame({"Data_UTC": pd.date_range(start=DATA_INICIO, end=DATA_FIM)})
                        .assign(Data_UTC = lambda df: pd.to_datetime(df['Data_UTC'].dt.strftime("%Y-%m-%d")))
                        .assign(is_weekend = lambda df: pd.to_datetime(df['Data_UTC']).dt.dayofweek.isin([5, 6]).astype(int))
                        .assign(Data_UTC = lambda df: df['Data_UTC'].dt.strftime("%Y-%m-%d"))
             )


# df_btc_price = pd.read_csv(rf"/Users/baia/Desktop/PYTHON/mba_dsa_usp_esalq/TCC/data/dados_btc/raw/price_btc.csv")
# df_btc_price['Data_UTC'] = pd.to_datetime(df_btc_price['time'], unit='s', utc=True,).dt.strftime("%Y-%m-%d")

# df_target_price =(
#     df_periodo
#         .merge(df_btc_price[['Data_UTC','close']], how='left', on='Data_UTC')
#         .assign(Data_UTC = lambda df: pd.to_datetime(df['Data_UTC']))
#         .rename(columns={'close':'btc_price'})
#         .assign(btc_log_ret = lambda df: np.log(df['btc_price']) - np.log(df['btc_price'].shift(1)))
#         .query("Data_UTC > '2016-12-31'")
#         [['Data_UTC','btc_price']]

# )
# df_target_price

df_features = pd.read_csv("/Users/baia/Desktop/PYTHON/mba_dsa_usp_esalq/TCC/data/tabela_consolidada.csv").query("Data_UTC > '2017-01-03'").drop_duplicates()
df_features['Data_UTC'] = pd.to_datetime(df_features['Data_UTC'])
# df_features = df_features.set_index('Data_UTC')


def print_dataframe_info(df, nome_df="DataFrame"):
    """
    Imprime informações sobre um DataFrame: describe, info e head.
    
    Parameters:
    df (pd.DataFrame): DataFrame a ser analisado
    nome_df (str): Nome do DataFrame para exibição
    """
    print(f"\n{'='*50}")
    print(f"Informações do {nome_df}")
    print(f"{'='*50}\n")
    
    print(f"DESCRIBE:")
    print(df.describe())
    print("\n" + "-"*50 + "\n")
    
    print(f"INFO:")
    print(df.info())
    print("\n" + "-"*50 + "\n")
    
    print(f"HEAD (10 primeiras linhas):")
    print(df.head(10))
    print(f"\n{'='*50}\n")