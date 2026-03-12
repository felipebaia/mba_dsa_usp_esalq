"""
=============================================================================
TABELA CONSOLIDADA DE FEATURES PARA O TCC
=============================================================================
Este script consolida todas as features extraídas dos notebooks de ETL em um 
único DataFrame, utilizando df_periodo como base para o merge (left join).

Features consolidadas:
    - BTC: RVI, Funding Rate, Miner Position, Exchange Supply, SPX-BTC Correlation, 
           Coinbase Premium, MVRV, Whale Transactions
    - MACRO: S&P500, DXY, NASDAQ, GOLD, US10Y, VIX
    - MKCAP: TOTAL3, SSR, Flippening Ratio
    - SOCIAL: Social Volume Spread, Momentum
    - STABLE: USDT Dominance, USDC Dominance
=============================================================================
"""

import sys
sys.path.append(rf"/Users/baia/Desktop/PYTHON/mba_dsa_usp_esalq")

from TCC.utils.constantes import *

# =============================================================================
# PATHS DOS ARQUIVOS RAW
# =============================================================================
PATH_BTC = "/Users/baia/Desktop/PYTHON/mba_dsa_usp_esalq/TCC/data/dados_btc/raw/"
PATH_ETH = "/Users/baia/Desktop/PYTHON/mba_dsa_usp_esalq/TCC/data/dados_eth/raw/"
PATH_MACRO = "/Users/baia/Desktop/PYTHON/mba_dsa_usp_esalq/TCC/data/dados_macro/raw/"
PATH_MKCAP = "/Users/baia/Desktop/PYTHON/mba_dsa_usp_esalq/TCC/data/dados_mkcap/raw/"
PATH_SOCIAL = "/Users/baia/Desktop/PYTHON/mba_dsa_usp_esalq/TCC/data/dados_social/raw/"
PATH_STABLE = "/Users/baia/Desktop/PYTHON/mba_dsa_usp_esalq/TCC/data/dados_stable/raw/"

# =============================================================================
# 1. FEATURES BTC
# =============================================================================

# --- 1.1 RVI (Relative Volatility Index) ---
df_btc_price = pd.read_csv(PATH_BTC + "price_btc.csv")
df_btc_price['Data_UTC'] = pd.to_datetime(df_btc_price['time'], unit='s', utc=True).dt.strftime("%Y-%m-%d")
df_btc_price = df_btc_price.rename(columns={'close': 'btc_price_close'})
df_btc_price = df_btc_price.assign(btc_log_ret=lambda df: np.log(df['btc_price_close'] / df['btc_price_close'].shift(1)))

df_btc_rvi = (
    df_periodo
    .merge(df_btc_price, how='left', on='Data_UTC')
    .assign(rvi_diff=lambda df: df['RVI'].diff())
    .query("Data_UTC > '2016-12-31'")
    .rename(columns={'RVI': 'rvi_close'})
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Data_UTC']).dt.strftime("%Y-%m-%d"))
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Data_UTC']))
    [['Data_UTC','btc_price_close', 'btc_log_ret', 'rvi_close', 'rvi_diff']]
)

# --- 1.2 Funding Rate (Futuros) ---
df_funding_cexs = pd.read_csv(PATH_BTC + "2017_fundingRateCEXs.csv")

df_funding_tratado = (
    df_funding_cexs
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Date']).dt.strftime("%Y-%m-%d"))
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Data_UTC']))
    .assign(
        total_funding_rate_btc=lambda df: df[[
            'BitMEX Funding Rate',
            'Binance Funding Rate (USDT)',
            'DyDx Exchange Funding Rates',
            'Deribit Exchange Funding Rates'
        ]].mean(axis=1, numeric_only=True),
        funding_rate_diff_btc=lambda df: df['total_funding_rate_btc'].diff()
    )
    .query("Data_UTC > '2016-12-31'")
    [['Data_UTC', 'total_funding_rate_btc', 'funding_rate_diff_btc']]
)

# --- 1.3 Miner Net Position Change (Supply Held by Miners) ---
df_supply_btc = pd.read_csv(PATH_BTC + "2009_supply_circulation_btc.csv")
df_supply_held_raw = pd.read_csv(PATH_BTC + "2010_supply_held_miners_whales.csv")

df_supply_held_by = (
    df_supply_held_raw
    .merge(df_supply_btc, how='left', on='Date')
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Date'], utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={
        'Total Supply': 'Total_Supply_btc',
        'Supply held by Miners': 'supply_held_by_miners_btc'
    })
    [['Data_UTC', 'supply_held_by_miners_btc', 'Total_Supply_btc']]
)

df_supply_held_by_miners_btc = (
    df_periodo
    .merge(df_supply_held_by, how='left', on='Data_UTC')
    .assign(miner_net_pos_change_log=lambda df: np.log(df['supply_held_by_miners_btc']) - np.log(df['supply_held_by_miners_btc'].shift(1)))
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Data_UTC']))
    .assign(Data_UTC=lambda df: df['Data_UTC'].dt.strftime("%Y-%m-%d"))
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Data_UTC']))
    .query("Data_UTC > '2016-12-31'")
    [['Data_UTC', 'supply_held_by_miners_btc', 'miner_net_pos_change_log']]
)

# --- 1.4 Supply on Exchanges (% of Total Supply) ---
df_historical_balance = (
    pd.read_csv(PATH_BTC + "2010_supply_on_exchanges_perc.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Date'], utc=True).dt.strftime("%Y-%m-%d"))
    [['Data_UTC', 'Supply on Exchanges (as % of total supply)']]
)

df_exchange_supply_btc = (
    df_periodo
    .merge(df_historical_balance, how='left', on='Data_UTC')
    .rename(columns={'Supply on Exchanges (as % of total supply)': 'supply_on_exchanges_perc_btc'})
    .assign(exchange_supply_diff_btc=lambda df: df['supply_on_exchanges_perc_btc'].diff())
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Data_UTC']))
    .assign(Data_UTC=lambda df: df['Data_UTC'].dt.strftime("%Y-%m-%d"))
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Data_UTC']))
    .query("Data_UTC > '2016-12-31'")
    [['Data_UTC', 'supply_on_exchanges_perc_btc', 'exchange_supply_diff_btc']]
)
# --- 1.5 Preço da ETH ---
df_eth_volume = (
    pd.read_csv(PATH_ETH + "2015_transactionVolumeEth.csv")
    .assign(Data_UTC = lambda df: pd.to_datetime(df['Date'], utc=True))
    .assign(Data_UTC = lambda df: df['Data_UTC'].dt.strftime("%Y-%m-%d"))
    .rename(columns={'Transaction Volume (ETH)': 'transaction_volume_eth',
                        'Transaction Volume USD (ETH)': 'Transaction_Volume_USD_ETH'}) 
    [['Data_UTC', 'transaction_volume_eth']] 
)

df_eth_volume_diff =(
    df_periodo
        .merge(df_eth_volume, how='left', on='Data_UTC')
        .assign(transaction_volume_eth = lambda df: df['transaction_volume_eth'].replace(0, np.nan))
        .assign(transaction_volume_eth = lambda df: df['transaction_volume_eth'].fillna(method='ffill'))
        .assign(eth_vol_log_ret = lambda df: np.log(df['transaction_volume_eth']) - np.log(df['transaction_volume_eth'].shift(1)))
        .query("Data_UTC > '2017-01-02'")
        [['Data_UTC','transaction_volume_eth','eth_vol_log_ret']]

)

# --- 1.6 Coinbase Premium Index ---
df_coinbase_premium = (
    pd.read_csv(PATH_BTC + "2018_coinbase_premium_index.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['time'], utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={'close': 'cb_premium_usd'})
    [['Data_UTC', 'cb_premium_usd']]
)

df_coinbase_premium_diff = (
    df_periodo
    .merge(df_coinbase_premium, how='left', on='Data_UTC')
    .assign(cb_premium_diff_btc=lambda df: df['cb_premium_usd'].diff())
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Data_UTC']))
    .query("Data_UTC >= '2020-03-11'")
    [['Data_UTC', 'cb_premium_usd', 'cb_premium_diff_btc']]
)

# --- 1.7 MVRV Z-Score ---
df_mvrv_raw = (
    pd.read_csv(PATH_BTC + "2017_mvrv_z_score.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['time'], utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={'Plot': 'mvrv_close'})
    .query("mvrv_close.isna() == False")
    [['Data_UTC', 'mvrv_close']]
)

df_mvrv_diff = (
    df_periodo
    # .assign(Data_UTC=lambda df: pd.to_datetime(df['Date'], utc=True))
    .merge(df_mvrv_raw, how='left', on='Data_UTC')
    .assign(mvrv_diff_btc=lambda df: df['mvrv_close'].diff())
    .query("Data_UTC > '2017-01-02'")
    [['Data_UTC', 'mvrv_close', 'mvrv_diff_btc']]
)

# --- 1.8 Whale Transaction Count ---
df_whale_transaction = (
    pd.read_csv(PATH_BTC + "2010_whale_transaction_count_btc_2.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Date'], utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={
        'Whale Transaction Count (>1m USD)': 'whale_transaction_count_1M_btc',
        'Whale Transaction Count (>100k USD)': 'whale_transaction_count_100k_btc'
    })
    [['Data_UTC', 'whale_transaction_count_1M_btc', 'whale_transaction_count_100k_btc']]
)

df_whale_transaction_log_ret = (
    df_periodo
    .merge(df_whale_transaction, how='left', on='Data_UTC')
    .fillna(0)
    .assign(
        whale_100k_log_ret=lambda df: np.log(df['whale_transaction_count_100k_btc'] + 1) - np.log(df['whale_transaction_count_100k_btc'].shift(1) + 1),
        whale_1m_log_ret=lambda df: np.log(df['whale_transaction_count_1M_btc'] + 1) - np.log(df['whale_transaction_count_1M_btc'].shift(1) + 1)
    )
    .query("Data_UTC > '2017-01-02'")
    [['Data_UTC', 'whale_100k_log_ret', 'whale_transaction_count_100k_btc', 'whale_1m_log_ret', 'whale_transaction_count_1M_btc']]
)

# =============================================================================
# 2. FEATURES MACRO
# =============================================================================

# --- 2.1 S&P 500 ---
df_sp500 = (
    pd.read_csv(PATH_MACRO + "2014_SP500_PRICE.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['time'], utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={'close': 'spx_close'})
    [['Data_UTC', 'spx_close']]
)

df_sp500_log_ret = (
    df_periodo
    .merge(df_sp500, how='left', on='Data_UTC')
    .assign(spx_close=lambda df: df['spx_close'].ffill())
    .assign(spx_log_ret=lambda df: np.log(df['spx_close']) - np.log(df['spx_close'].shift(1)))
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Data_UTC']))
    .query("Data_UTC > '2017-01-03'")
    [['Data_UTC', 'spx_close', 'spx_log_ret']]
)

# --- 2.2 DXY (US Dollar Index) ---
df_dxy = (
    pd.read_csv(PATH_MACRO + "dxy_new.csv")
    .rename(columns={'dxy_close_new': 'dxy_close'})
    [['Data_UTC', 'dxy_close']]
)

df_dxy_log_ret = (
    df_periodo
    .merge(df_dxy, how='left', on='Data_UTC')
    .assign(dxy_close=lambda df: df['dxy_close'].ffill())
    .assign(dxy_log_ret=lambda df: np.log(df['dxy_close']) - np.log(df['dxy_close'].shift(1)))
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Data_UTC']))
    .query("Data_UTC > '2017-01-03'")
    [['Data_UTC', 'dxy_close', 'dxy_log_ret']]
)

# --- 2.3 NASDAQ ---
df_nasdaq = (
    pd.read_csv(PATH_MACRO + "nasdaq.csv")
    .assign(Data_UTC = lambda df: pd.to_datetime(df['Date']).dt.strftime("%Y-%m-%d"))
    .rename(columns={'Close/Last': 'nasdaq_close'}) 

    [['Data_UTC', 'nasdaq_close']] 
)

df_nasdaq_log_ret = (
    df_periodo
    .merge(df_nasdaq, how='left', on='Data_UTC')
    .assign(nasdaq_close = lambda df: df['nasdaq_close'].ffill())
    .assign(nasdaq_log_ret = lambda df: np.log(df['nasdaq_close']) - np.log(df['nasdaq_close'].shift(1)))
    .assign(Data_UTC = lambda df: pd.to_datetime(df['Data_UTC']))
    .query("Data_UTC > '2017-01-03'")
    [['Data_UTC','nasdaq_close','nasdaq_log_ret']]
)

# --- 2.4 GOLD ---
df_gold = (
    pd.read_csv(PATH_MACRO + "201501_PRICE_GOLD.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['time'], unit='s', utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={'close': 'gold_close'})
    [['Data_UTC', 'gold_close']]
)

df_gold_log_ret = (
    df_periodo
    .merge(df_gold, how='left', on='Data_UTC')
    .assign(gold_close=lambda df: df['gold_close'].ffill())
    .assign(gold_log_ret=lambda df: np.log(df['gold_close']) - np.log(df['gold_close'].shift(1)))
    .query("Data_UTC > '2017-01-03'")
    [['Data_UTC', 'gold_close', 'gold_log_ret']]
)

# --- 2.5 US10Y (Treasury Yield) ---
df_us10y = (
    pd.read_csv(PATH_MACRO + "201501_US10Y.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['time'], unit='s', utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={'close': 'us10y_close'})
    [['Data_UTC', 'us10y_close']]
)

df_us10y_diff = (
    df_periodo
    .merge(df_us10y, how='left', on='Data_UTC')
    .assign(us10y_close=lambda df: df['us10y_close'].ffill())
    .assign(us10y_diff=lambda df: df['us10y_close'].diff())
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Data_UTC']))
    .query("Data_UTC > '2017-01-03'")
    [['Data_UTC', 'us10y_close', 'us10y_diff']]
)

# --- 2.6 VIX ---
df_vix = (
    pd.read_csv(PATH_MACRO + "201501_VIX.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['time'], unit='s', utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={'close': 'vix_close'})
    [['Data_UTC', 'vix_close']]
)

df_vix_log_ret = (
    df_periodo
    .merge(df_vix, how='left', on='Data_UTC')
    .assign(vix_close=lambda df: df['vix_close'].ffill())
    .assign(vix_log_ret=lambda df: np.log(df['vix_close']) - np.log(df['vix_close'].shift(1)))
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Data_UTC']))
    .query("Data_UTC > '2017-01-03'")
    [['Data_UTC', 'vix_close', 'vix_log_ret']]
)

# =============================================================================
# 3. FEATURES MKCAP
# =============================================================================

# --- 3.1 TOTAL3 (Altcoin Market Cap excl. BTC, ETH, Stables) ---
df_total3 = (
    pd.read_csv(PATH_MKCAP + "201501_mkcap_total3.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['time'], unit='s', utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={'close': 'total3_close'})
    [['Data_UTC', 'total3_close']]
)

df_total3_log_ret = (
    df_periodo
    .merge(df_total3, how='left', on='Data_UTC')
    .assign(total3_close=lambda df: df['total3_close'].ffill())
    .assign(total3_log_ret=lambda df: np.log(df['total3_close']) - np.log(df['total3_close'].shift(1)))
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Data_UTC']))
    .query("Data_UTC > '2017-01-03'")
    [['Data_UTC', 'total3_close', 'total3_log_ret']]
)

# --- 3.2 SSR (Stablecoin Supply Ratio) ---
df_ssr = (
    pd.read_csv(PATH_MKCAP + "201404_ssr.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['time'], utc=True).dt.strftime("%Y-%m-%d"))
    [['Data_UTC', 'SSR']]
)

df_ssr_diff = (
    df_periodo
    .merge(df_ssr, how='left', on='Data_UTC')
    .assign(ssr=lambda df: df['SSR'].ffill())
    .assign(ssr_diff=lambda df: df['ssr'].diff())
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Data_UTC']))
    .query("Data_UTC > '2017-01-03'")
    [['Data_UTC', 'ssr', 'ssr_diff']]
)

# --- 3.3 Flippening Ratio (ETH/BTC) ---
df_flippening = (
    pd.read_csv(PATH_MKCAP + "201809_Flippening_Ratio_Diff.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['time'], utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={'close': 'flippening_close'})
    [['Data_UTC', 'flippening_close']]
)

df_flippening_diff = (
    df_periodo
    .merge(df_flippening, how='left', on='Data_UTC')
    .assign(flippening_close=lambda df: df['flippening_close'].ffill())
    .assign(flippening_close_diff=lambda df: df['flippening_close'].diff())
    .query("Data_UTC > '2020-01-01'")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Data_UTC']))
    [['Data_UTC', 'flippening_close', 'flippening_close_diff']]
)

# =============================================================================
# 4. FEATURES SOCIAL
# =============================================================================

df_social = pd.read_csv(PATH_SOCIAL + "2012_btc_altcoin_social_metrics.csv")
df_social['Data_UTC'] = pd.to_datetime(df_social['Date'], utc=True).dt.strftime("%Y-%m-%d")

df_social = df_social.rename(columns={
    'BTC / USD': 'btc_price',
    'Social Volume (btc OR bitcoin)': 'btc_social_volume',
    'Social Dominance (btc OR bitcoin)': 'btc_social_dominance',
    'Social Volume (altcoin OR altcoins OR "altcoin season" OR "altcoin pump" OR "altcoin rally")': 'alt_social_volume',
    'Social Dominance (altcoin OR altcoins OR "altcoin season" OR "altcoin pump" OR "altcoin rally")': 'alt_social_dominance',
    'Social Volume (ethereum OR eth OR solana OR sol OR cardano OR ada OR polkadot OR dot OR ripple OR xrp OR dogecoin OR doge OR memecoin OR shiba)': 'alt_social_volume_2',
    'Social Dominance (ethereum OR eth OR solana OR sol OR cardano OR ada OR polkadot OR dot OR ripple OR xrp OR dogecoin OR doge OR memecoin OR shiba)': 'alt_social_dominance_2',
})

df_social_tratado = (
    df_periodo
        .merge(df_social, how='left', on='Data_UTC')
        .sort_values('Data_UTC')
        .assign(Data_UTC = lambda df: pd.to_datetime(df['Data_UTC']))
        
        # 1. Tratamento de Volume (Escala Logarítmica para normalizar contagem)
        .assign(btc_log_vol = lambda df: np.log1p(df['btc_social_volume']))
        .assign(alt_total_log_vol = lambda df: np.log1p(df['alt_social_volume'] + df['alt_social_volume_2']))
        
        # 4. Intensidade do Barulho (Z-Score ou Spread de Volume)
        # Indica se o mercado está em um momento de alta atividade (Hype)
        .assign(social_vol_spread = lambda df: df['btc_log_vol'] - df['alt_total_log_vol'])
        
        # 5. Momentum e Aceleração (Derivadas para Séries Temporais)
        # Útil para modelos de Causalidade de Granger ou Regressões Dinâmicas
        .assign(vol_acceleration = lambda df: df['social_vol_spread'].diff())

        .assign(social_vol_spread_ma7 = lambda df: df['social_vol_spread'].rolling(window=7).mean())

        .fillna(0)

        # Seleção das colunas chave para o modelo de Data Science
        [[
            'Data_UTC',
            'btc_social_volume',
            'btc_log_vol',
            'alt_total_log_vol',
            'social_vol_spread',       # Intensidade relativa (Volume)
            'vol_acceleration'         # Explosão de interesse
        ]]
)

df_social_tratado

# =============================================================================
# 5. FEATURES STABLECOINS
# =============================================================================

# --- 5.1 USDT Dominance ---
df_usdt_raw = (
    pd.read_csv(PATH_STABLE + "201501_dominance_usdt.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['time'], unit='s', utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={'close': 'usdt_dominance'})
    [['Data_UTC', 'usdt_dominance']]
)

df_usdt_log_ret = (
    df_periodo
    .merge(df_usdt_raw, how='left', on='Data_UTC')
    .assign(usdt_dominance=lambda df: df['usdt_dominance'].ffill())
    .assign(usdt_log_ret=lambda df: np.log(df['usdt_dominance']) - np.log(df['usdt_dominance'].shift(1)))
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Data_UTC']))
    .query("Data_UTC > '2017-01-03'")
    [['Data_UTC', 'usdt_dominance', 'usdt_log_ret']]
)


# =============================================================================
# CONSOLIDAÇÃO FINAL - LEFT JOIN DE TODAS AS FEATURES
# =============================================================================

# Função auxiliar para garantir Data_UTC como string
def to_str_date(df):
    df = df.copy()
    if pd.api.types.is_datetime64_any_dtype(df['Data_UTC']):
        df['Data_UTC'] = df['Data_UTC'].dt.strftime("%Y-%m-%d")
    return df

# Preparar df_periodo com is_weekend como base (Data_UTC como string)
df_base = df_periodo[['Data_UTC', 'is_weekend']].copy()

df_consolidado = (
    df_base
    # BTC Features
    .merge(to_str_date(df_btc_rvi), how='left', on='Data_UTC')
    .merge(to_str_date(df_funding_tratado), how='left', on='Data_UTC')
    .merge(to_str_date(df_supply_held_by_miners_btc), how='left', on='Data_UTC')
    .merge(to_str_date(df_exchange_supply_btc), how='left', on='Data_UTC')
    .merge(to_str_date(df_coinbase_premium_diff), how='left', on='Data_UTC')
    .merge(to_str_date(df_mvrv_diff), how='left', on='Data_UTC')
    .merge(to_str_date(df_whale_transaction_log_ret), how='left', on='Data_UTC')
    # ETH Features
    .merge(to_str_date(df_eth_volume_diff), how='left', on='Data_UTC')
    # MACRO Features
    .merge(to_str_date(df_sp500_log_ret), how='left', on='Data_UTC')
    .merge(to_str_date(df_dxy_log_ret), how='left', on='Data_UTC')
    .merge(to_str_date(df_nasdaq_log_ret), how='left', on='Data_UTC')
    .merge(to_str_date(df_gold_log_ret), how='left', on='Data_UTC')
    .merge(to_str_date(df_us10y_diff), how='left', on='Data_UTC')
    .merge(to_str_date(df_vix_log_ret), how='left', on='Data_UTC')
    # MKCAP Features
    .merge(to_str_date(df_total3_log_ret), how='left', on='Data_UTC')
    .merge(to_str_date(df_ssr_diff), how='left', on='Data_UTC')
    .merge(to_str_date(df_flippening_diff), how='left', on='Data_UTC')
    # SOCIAL Features
    .merge(to_str_date(df_social_tratado), how='left', on='Data_UTC')
    # STABLE Features
    .merge(to_str_date(df_usdt_log_ret), how='left', on='Data_UTC')
)

# Converter Data_UTC para datetime no final
df_consolidado['Data_UTC'] = pd.to_datetime(df_consolidado['Data_UTC'])

# =============================================================================
# DICIONÁRIO DE FEATURES (REFERÊNCIA)
# =============================================================================
"""
FEATURES CONSOLIDADAS:
======================

BASE:
-----
- Data_UTC: Data em formato datetime
- is_weekend: Dummy indicando se é fim de semana (1 = Sábado/Domingo, 0 = Dia útil)

BTC (Bitcoin On-Chain & Technical):
-----------------------------------
- rvi_close: Valor do RVI (Relative Volatility Index)
- rvi_diff: Diff do RVI - Direção da volatilidade
- total_funding_rate_btc: Média das Funding Rates (BitMEX, Binance, DyDx, Deribit)
- funding_rate_diff_btc: Diff da Funding Rate - Mudança na alavancagem
- supply_held_by_miners_btc: Supply mantido pelos mineradores
- miner_net_pos_change: Log-retorno do Supply dos Mineradores - Acumulação/Distribuição
- supply_on_exchanges_perc_btc: Supply em Exchanges (% do total)
- exchange_supply_diff_btc: Diff do Supply em Exchanges (%) - Fluxo para/das exchanges
# - spx_price: Preço do S&P 500 (para correlação)
- btc_log_ret: Log-retorno do preço do BTC
- btc_spx_corr_30d: Correlação rolling 30d entre BTC e S&P500
- btcmium_usd: Coinbase Premium em USD
- cb_premium_diff_btc: Diff do Coinbase Premium - Demanda institucional US
- mvrv_close: Valor do MVRV Z-Score
- mvrv_diff_btc: Diff do MVRV Z-Score - Sobre/subvalorização
- whale_100k_log_ret: Log-retorno de transações whale >100k USD
- whale_transaction_count_100k_btc: Contagem de transações whale >100k USD
- whale_1m_log_ret: Log-retorno de transações whale >1M USD
- whale_transaction_count_1M_btc: Contagem de transações whale >1M USD

MACRO (Indicadores Macroeconômicos):
------------------------------------
- spx_close: Preço de fechamento do S&P 500
- spx_log_ret: Log-retorno do S&P 500
- dxy_close: Preço de fechamento do DXY (US Dollar Index)
- dxy_log_ret: Log-retorno do DXY
- ndx_close: Preço de fechamento do NASDAQ
- ndx_log_ret: Log-retorno do NASDAQ
- gold_close: Preço de fechamento do Ouro
- gold_log_ret: Log-retorno do Ouro
- us10y_close: Yield dos títulos US10Y
- us10y_diff: Diff da taxa de juros US10Y (pontos base)
- vix_close: Preço de fechamento do VIX (Índice do Medo)
- vix_log_ret: Log-retorno do VIX

MKCAP (Market Cap Metrics):
---------------------------
- total3_close: Market Cap do TOTAL3 (Altcoins excl. BTC/ETH/Stables)
- total3_log_ret: Log-retorno do TOTAL3
- ssr: Stablecoin Supply Ratio
- ssr_diff: Diff do SSR - Poder de compra
- flippening_close: Razão ETH/BTC (Flippening Ratio)
- flippening_close_diff: Diff do Flippening Ratio

SOCIAL (Sentimento de Mercado):
-------------------------------
- social_vol_log_spread: Spread log do volume social BTC vs Altcoins
- social_momentum: Diff do spread (aceleração do hype)

STABLECOINS (Liquidez):
-----------------------
- usdt_dominance: Dominância do USDT (%)
- usdt_log_ret: Log-retorno da dominância do USDT
- usdc_dominance: Dominância do USDC (%)
- usdc_log_ret: Log-retorno da dominância do USDC
"""

# =============================================================================
# EXIBIÇÃO E VALIDAÇÃO
# =============================================================================

if __name__ == "__main__":
    print("\n" + "="*80)
    print("TABELA CONSOLIDADA DE FEATURES")
    print("="*80)
    
    print(f"\nShape: {df_consolidado.shape}")
    print(f"Período: {df_consolidado['Data_UTC'].min()} a {df_consolidado['Data_UTC'].max()}")
    
    print("\n" + "-"*40)
    print("COLUNAS DISPONÍVEIS:")
    print("-"*40)
    for i, col in enumerate(df_consolidado.columns, 1):
        print(f"  {i:2}. {col}")
    
    print("\n" + "-"*40)
    print("ESTATÍSTICAS DESCRITIVAS:")
    print("-"*40)
    print(df_consolidado.describe().T)
    
    print("\n" + "-"*40)
    print("VALORES NULOS POR COLUNA:")
    print("-"*40)
    null_counts = df_consolidado.isnull().sum()
    print(null_counts[null_counts > 0])
    
    print("\n" + "-"*40)
    print("AMOSTRA DOS DADOS (HEAD 10):")
    print("-"*40)
    print(df_consolidado.head(10))

    df_consolidado.to_csv(PATH_TABELA_CONSOLIDADA, index=False)
    print(f"\nTabela consolidada salva em: {PATH_TABELA_CONSOLIDADA}")
