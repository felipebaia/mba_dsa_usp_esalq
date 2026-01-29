"""
=============================================================================
TABELA CONSOLIDADA DE FEATURES PARA O TCC
=============================================================================
Este script consolida todas as features extraídas dos notebooks de ETL em um 
único DataFrame, utilizando df_periodo como base para o merge (left join).

Features consolidadas:
    - BTC: RVI, HV, Funding Rate, Miner Position, Exchange Supply, SPX-BTC Correlation, 
           Coinbase Premium, MVRV, Whale Transactions
    - ETH: Transaction Volume, Exchange Supply, Social Metrics
    - MACRO: S&P500, DXY, NASDAQ, GOLD, US10Y, VIX
    - MKCAP: TOTAL3, SSR, Flippening Ratio
    - SOCIAL: Social Volume Spread, Momentum, Market Noise
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

# =============================================================================
# 1. FEATURES BTC
# =============================================================================

# --- 1.1 RVI (Relative Volatility Index) & HV (Historical Volatility) ---
df_btc_price = pd.read_csv(PATH_BTC + "price_btc.csv")
df_btc_price['Data_UTC'] = pd.to_datetime(df_btc_price['time'], unit='s', utc=True).dt.strftime("%Y-%m-%d")

df_btc_rvi = (
    df_periodo
    .merge(df_btc_price, how='left', on='Data_UTC')
    .assign(btc_rvi_diff=lambda df: df['RVI'].diff())
    [['Data_UTC', 'btc_rvi_diff']]
)

df_btc_hv = (
    df_periodo
    .merge(df_btc_price, how='left', on='Data_UTC')
    .assign(btc_hv_log_ret=lambda df: np.log1p(df['HV']) - np.log1p(df['HV'].shift(1)))
    [['Data_UTC', 'btc_hv_log_ret']]
)

# --- 1.2 Funding Rate (Futuros) ---
df_funding_cexs = pd.read_csv(PATH_BTC + "2017_fundingRateCEXs.csv")
df_funding_tratado = (
    df_funding_cexs
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Date']).dt.strftime("%Y-%m-%d"))
    .assign(
        btc_funding_rate_mean=lambda df: df[[
            'BitMEX Funding Rate',
            'Binance Funding Rate (USDT)',
            'DyDx Exchange Funding Rates',
            'Deribit Exchange Funding Rates'
        ]].mean(axis=1, numeric_only=True),
        btc_funding_rate_diff=lambda df: df['btc_funding_rate_mean'].diff()
    )
    [['Data_UTC', 'btc_funding_rate_mean', 'btc_funding_rate_diff']]
)

# --- 1.3 Miner Net Position Change ---
df_supply_btc = pd.read_csv(PATH_BTC + "2009_supply_circulation_btc.csv")
df_supply_held_raw = pd.read_csv(PATH_BTC + "2010_supply_held_miners_whales.csv")

df_supply_held_by = (
    df_supply_held_raw
    .merge(df_supply_btc, how='left', on='Date')
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Date'], utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={
        'Total Supply': 'btc_total_supply',
        'Supply held by Miners': 'btc_supply_held_miners'
    })
    [['Data_UTC', 'btc_supply_held_miners', 'btc_total_supply']]
)

df_miner_position = (
    df_periodo
    .merge(df_supply_held_by, how='left', on='Data_UTC')
    .assign(btc_miner_net_pos_change=lambda df: np.log1p(df['btc_supply_held_miners']) - np.log1p(df['btc_supply_held_miners'].shift(1)))
    [['Data_UTC', 'btc_miner_net_pos_change']]
)

# --- 1.4 Supply on Exchanges (% of Total Supply) ---
df_historical_balance = (
    pd.read_csv(PATH_BTC + "2010_supply_on_exchanges_perc.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Date'], utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={'Supply on Exchanges (as % of total supply)': 'btc_supply_on_exchanges_pct'})
    [['Data_UTC', 'btc_supply_on_exchanges_pct']]
)

df_btc_exchange_supply = (
    df_periodo
    .merge(df_historical_balance, how='left', on='Data_UTC')
    .assign(btc_exchange_supply_diff=lambda df: df['btc_supply_on_exchanges_pct'].diff())
    [['Data_UTC', 'btc_exchange_supply_diff']]
)

# --- 1.5 SPX-BTC Rolling Correlation (30d) ---
df_sp500_raw = (
    pd.read_csv(PATH_MACRO + "2014_SP500_PRICE.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['time'], utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={'close': 'sp500_price'})
    [['Data_UTC', 'sp500_price']]
)

df_btc_spx_corr = (
    df_periodo
    .merge(df_sp500_raw, how='left', on='Data_UTC')
    .merge(df_btc_price[['Data_UTC', 'close']], how='left', on='Data_UTC')
    .assign(sp500_price=lambda df: df['sp500_price'].ffill())
    .assign(btc_log_ret=lambda df: np.log1p(df['close']) - np.log1p(df['close'].shift(1)))
    .assign(spx_log_ret=lambda df: np.log1p(df['sp500_price']) - np.log1p(df['sp500_price'].shift(1)))
    .assign(btc_spx_corr_30d=lambda df: df['btc_log_ret'].rolling(window=30).corr(df['spx_log_ret']))
    .assign(btc_spx_corr_30d=lambda df: df['btc_spx_corr_30d'].bfill())
    [['Data_UTC', 'btc_log_ret', 'btc_spx_corr_30d']]
)

# --- 1.6 Coinbase Premium Index ---
df_coinbase_premium = (
    pd.read_csv(PATH_BTC + "2018_coinbase_premium_index.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['time'], utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={'close': 'btc_coinbase_premium_usd'})
    [['Data_UTC', 'btc_coinbase_premium_usd']]
)

df_coinbase_premium_diff = (
    df_periodo
    .merge(df_coinbase_premium, how='left', on='Data_UTC')
    .assign(btc_coinbase_premium_diff=lambda df: df['btc_coinbase_premium_usd'].diff())
    [['Data_UTC', 'btc_coinbase_premium_diff']]
)

# --- 1.7 MVRV Z-Score ---
df_mvrv_raw = (
    pd.read_csv(PATH_BTC + "2017_mvrv_z_score.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['time'], utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={'Plot': 'btc_mvrv'})
    .query("btc_mvrv.notna()")
    [['Data_UTC', 'btc_mvrv']]
)

df_mvrv = (
    df_periodo
    .merge(df_mvrv_raw, how='left', on='Data_UTC')
    .assign(btc_mvrv_diff=lambda df: df['btc_mvrv'].diff())
    [['Data_UTC', 'btc_mvrv_diff']]
)

# --- 1.8 Whale Transaction Count ---
df_whale_raw = (
    pd.read_csv(PATH_BTC + "2010_whale_transaction_count_btc_2.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Date'], utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={
        'Whale Transaction Count (>1m USD)': 'btc_whale_tx_1m',
        'Whale Transaction Count (>100k USD)': 'btc_whale_tx_100k'
    })
    [['Data_UTC', 'btc_whale_tx_1m', 'btc_whale_tx_100k']]
)

df_whale = (
    df_periodo
    .merge(df_whale_raw, how='left', on='Data_UTC')
    .fillna(0)
    .assign(
        btc_whale_100k_log_ret=lambda df: np.log1p(df['btc_whale_tx_100k']) - np.log1p(df['btc_whale_tx_100k'].shift(1)),
        btc_whale_1m_log_ret=lambda df: np.log1p(df['btc_whale_tx_1m']) - np.log1p(df['btc_whale_tx_1m'].shift(1))
    )
    [['Data_UTC', 'btc_whale_100k_log_ret', 'btc_whale_1m_log_ret']]
)

# =============================================================================
# 2. FEATURES ETH
# =============================================================================

# --- 2.1 Transaction Volume ETH ---
df_eth_volume_raw = (
    pd.read_csv(PATH_ETH + "2015_transactionVolumeEth.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Date'], utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={'Transaction Volume (ETH)': 'eth_tx_volume'})
    [['Data_UTC', 'eth_tx_volume']]
)

df_eth_volume = (
    df_periodo
    .merge(df_eth_volume_raw, how='left', on='Data_UTC')
    .assign(eth_tx_volume=lambda df: df['eth_tx_volume'].fillna(0))
    .assign(eth_vol_log_ret=lambda df: np.log1p(df['eth_tx_volume']) - np.log1p(df['eth_tx_volume'].shift(1)))
    [['Data_UTC', 'eth_vol_log_ret']]
)

# --- 2.2 ETH Exchange Supply (% of Total Supply) ---
df_eth_supply_raw = (
    pd.read_csv(PATH_ETH + "2015_supply_held_by_exchanges_top_wallets_2.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Date'], utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={'Supply on Exchanges (as % of total supply) (ETH)': 'eth_supply_on_exchanges_pct'})
    [['Data_UTC', 'eth_supply_on_exchanges_pct']]
)

df_eth_exchange_supply = (
    df_periodo
    .merge(df_eth_supply_raw, how='left', on='Data_UTC')
    .assign(eth_exchange_supply_diff=lambda df: df['eth_supply_on_exchanges_pct'].diff())
    [['Data_UTC', 'eth_exchange_supply_diff']]
)

# --- 2.3 Social Metrics ETH ---
df_social_eth_raw = (
    pd.read_csv(PATH_ETH + "eth_social_metrics.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['Date'], utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={
        'Social Dominance (ETH)': 'eth_social_dominance',
        'Social Volume (ETH)': 'eth_social_volume'
    })
    [['Data_UTC', 'eth_social_dominance', 'eth_social_volume']]
)

df_eth_social = (
    df_periodo
    .merge(df_social_eth_raw, how='left', on='Data_UTC')
    .assign(eth_social_dom_diff=lambda df: df['eth_social_dominance'].diff())
    .assign(eth_social_vol_log_ret=lambda df: np.log1p(df['eth_social_volume']) - np.log1p(df['eth_social_volume'].shift(1)))
    [['Data_UTC', 'eth_social_dom_diff', 'eth_social_vol_log_ret']]
)

# =============================================================================
# 3. FEATURES MACRO
# =============================================================================

# --- 3.1 S&P 500 ---
df_sp500 = (
    pd.read_csv(PATH_MACRO + "2014_SP500_PRICE.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['time'], utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={'close': 'spx_close'})
    [['Data_UTC', 'spx_close']]
)

df_spx_log_ret = (
    df_periodo
    .merge(df_sp500, how='left', on='Data_UTC')
    .assign(spx_close=lambda df: df['spx_close'].ffill())
    .assign(spx_log_ret=lambda df: np.log1p(df['spx_close']) - np.log1p(df['spx_close'].shift(1)))
    [['Data_UTC', 'spx_log_ret']]
)

# --- 3.2 DXY (US Dollar Index) ---
df_dxy = (
    pd.read_csv(PATH_MACRO + "201501_DXY.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['time'], unit='s', utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={'close': 'dxy_close'})
    [['Data_UTC', 'dxy_close']]
)

df_dxy_log_ret = (
    df_periodo
    .merge(df_dxy, how='left', on='Data_UTC')
    .assign(dxy_close=lambda df: df['dxy_close'].ffill())
    .assign(dxy_log_ret=lambda df: np.log1p(df['dxy_close']) - np.log1p(df['dxy_close'].shift(1)))
    [['Data_UTC', 'dxy_log_ret']]
)

# --- 3.3 NASDAQ ---
df_nasdaq = (
    pd.read_csv(PATH_MACRO + "201501_NASDAQ.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['time'], unit='s', utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={'close': 'nasdaq_close'})
    [['Data_UTC', 'nasdaq_close']]
)

df_nasdaq_log_ret = (
    df_periodo
    .merge(df_nasdaq, how='left', on='Data_UTC')
    .assign(nasdaq_close=lambda df: df['nasdaq_close'].ffill())
    .assign(nasdaq_log_ret=lambda df: np.log1p(df['nasdaq_close']) - np.log1p(df['nasdaq_close'].shift(1)))
    [['Data_UTC', 'nasdaq_log_ret']]
)

# --- 3.4 GOLD ---
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
    .assign(gold_log_ret=lambda df: np.log1p(df['gold_close']) - np.log1p(df['gold_close'].shift(1)))
    [['Data_UTC', 'gold_log_ret']]
)

# --- 3.5 US10Y (Treasury Yield) ---
df_us10y = (
    pd.read_csv(PATH_MACRO + "201501_US10Y.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['time'], unit='s', utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={'close': 'us10y_yield'})
    [['Data_UTC', 'us10y_yield']]
)

df_us10y_diff = (
    df_periodo
    .merge(df_us10y, how='left', on='Data_UTC')
    .assign(us10y_yield=lambda df: df['us10y_yield'].ffill())
    .assign(us10y_diff=lambda df: df['us10y_yield'].diff())
    [['Data_UTC', 'us10y_diff']]
)

# --- 3.6 VIX ---
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
    .assign(vix_log_ret=lambda df: np.log1p(df['vix_close']) - np.log1p(df['vix_close'].shift(1)))
    [['Data_UTC', 'vix_log_ret']]
)

# =============================================================================
# 4. FEATURES MKCAP
# =============================================================================

# --- 4.1 TOTAL3 (Altcoin Market Cap excl. BTC, ETH, Stables) ---
df_total3 = (
    pd.read_csv(PATH_MKCAP + "201501_mkcap_total3.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['time'], unit='s', utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={'close': 'total3_mkcap'})
    [['Data_UTC', 'total3_mkcap']]
)

df_total3_log_ret = (
    df_periodo
    .merge(df_total3, how='left', on='Data_UTC')
    .assign(total3_mkcap=lambda df: df['total3_mkcap'].ffill())
    .assign(total3_log_ret=lambda df: np.log1p(df['total3_mkcap']) - np.log1p(df['total3_mkcap'].shift(1)))
    [['Data_UTC', 'total3_log_ret']]
)

# --- 4.2 SSR (Stablecoin Supply Ratio) ---
df_ssr = (
    pd.read_csv(PATH_MKCAP + "201404_ssr.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['time'], utc=True).dt.strftime("%Y-%m-%d"))
    [['Data_UTC', 'SSR']]
)

df_ssr_diff = (
    df_periodo
    .merge(df_ssr, how='left', on='Data_UTC')
    .assign(SSR=lambda df: df['SSR'].ffill())
    .assign(ssr_diff=lambda df: df['SSR'].diff())
    [['Data_UTC', 'ssr_diff']]
)

# --- 4.3 Flippening Ratio (ETH/BTC) ---
df_flippening = (
    pd.read_csv(PATH_MKCAP + "201809_Flippening_Ratio_Diff.csv")
    .assign(Data_UTC=lambda df: pd.to_datetime(df['time'], utc=True).dt.strftime("%Y-%m-%d"))
    .rename(columns={'close': 'flippening_ratio'})
    [['Data_UTC', 'flippening_ratio']]
)

df_flippening_diff = (
    df_periodo
    .merge(df_flippening, how='left', on='Data_UTC')
    .assign(flippening_ratio=lambda df: df['flippening_ratio'].ffill())
    .assign(flippening_ratio_diff=lambda df: df['flippening_ratio'].diff())
    [['Data_UTC', 'flippening_ratio_diff']]
)

# =============================================================================
# 5. FEATURES SOCIAL
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
    # Transformação Logarítmica
    .assign(btc_log_vol=lambda df: np.log1p(df['btc_social_volume']))
    .assign(alt_log_vol_1=lambda df: np.log1p(df['alt_social_volume']))
    .assign(alt_log_vol_2=lambda df: np.log1p(df['alt_social_volume_2']))
    # Social Volume e Dominance (Soma dos Altcoins)
    .assign(alt_total_log_vol=lambda df: df['alt_log_vol_1'] + df['alt_log_vol_2'])
    # Spread de Intensidade (BTC vs Alts)
    .assign(social_vol_log_spread=lambda df: df['btc_log_vol'] - df['alt_total_log_vol'])
    # Momentum (Derivada do Spread)
    .assign(social_momentum=lambda df: df['social_vol_log_spread'].diff())
    # Z-Score Total do Mercado (Euforia vs Apatia)
    .assign(total_market_noise_z=lambda df:
            ((df['btc_log_vol'] + df['alt_total_log_vol']) - (df['btc_log_vol'] + df['alt_total_log_vol']).rolling(30).mean()) /
            (df['btc_log_vol'] + df['alt_total_log_vol']).rolling(30).std())
    .fillna(0)
    [['Data_UTC', 'social_vol_log_spread', 'social_momentum', 'total_market_noise_z']]
)

# =============================================================================
# CONSOLIDAÇÃO FINAL - LEFT JOIN DE TODAS AS FEATURES
# =============================================================================

df_consolidado = (
    df_periodo
    # BTC Features
    .merge(df_btc_rvi, how='left', on='Data_UTC')
    .merge(df_btc_hv, how='left', on='Data_UTC')
    .merge(df_funding_tratado, how='left', on='Data_UTC')
    .merge(df_miner_position, how='left', on='Data_UTC')
    .merge(df_btc_exchange_supply, how='left', on='Data_UTC')
    .merge(df_btc_spx_corr, how='left', on='Data_UTC')
    .merge(df_coinbase_premium_diff, how='left', on='Data_UTC')
    .merge(df_mvrv, how='left', on='Data_UTC')
    .merge(df_whale, how='left', on='Data_UTC')
    # ETH Features
    .merge(df_eth_volume, how='left', on='Data_UTC')
    .merge(df_eth_exchange_supply, how='left', on='Data_UTC')
    .merge(df_eth_social, how='left', on='Data_UTC')
    # MACRO Features
    .merge(df_spx_log_ret, how='left', on='Data_UTC')
    .merge(df_dxy_log_ret, how='left', on='Data_UTC')
    .merge(df_nasdaq_log_ret, how='left', on='Data_UTC')
    .merge(df_gold_log_ret, how='left', on='Data_UTC')
    .merge(df_us10y_diff, how='left', on='Data_UTC')
    .merge(df_vix_log_ret, how='left', on='Data_UTC')
    # MKCAP Features
    .merge(df_total3_log_ret, how='left', on='Data_UTC')
    .merge(df_ssr_diff, how='left', on='Data_UTC')
    .merge(df_flippening_diff, how='left', on='Data_UTC')
    # SOCIAL Features
    .merge(df_social_tratado, how='left', on='Data_UTC')
)

# =============================================================================
# DICIONÁRIO DE FEATURES (REFERÊNCIA)
# =============================================================================
"""
FEATURES CONSOLIDADAS:
======================

BTC (Bitcoin On-Chain & Technical):
-----------------------------------
- btc_rvi_diff: Diff do RVI (Relative Volatility Index) - Direção da volatilidade
- btc_hv_log_ret: Log-retorno da Volatilidade Histórica
- btc_funding_rate_mean: Média das Funding Rates (BitMEX, Binance, DyDx, Deribit)
- btc_funding_rate_diff: Diff da Funding Rate - Mudança na alavancagem
- btc_miner_net_pos_change: Log-retorno do Supply dos Mineradores - Acumulação/Distribuição
- btc_exchange_supply_diff: Diff do Supply em Exchanges (%) - Fluxo para/das exchanges
- btc_log_ret: Log-retorno do preço do BTC
- btc_spx_corr_30d: Correlação rolling 30d entre BTC e S&P500
- btc_coinbase_premium_diff: Diff do Coinbase Premium - Demanda institucional US
- btc_mvrv_diff: Diff do MVRV Z-Score - Sobre/subvalorização
- btc_whale_100k_log_ret: Log-retorno de transações whale >100k USD
- btc_whale_1m_log_ret: Log-retorno de transações whale >1M USD

ETH (Ethereum On-Chain):
------------------------
- eth_vol_log_ret: Log-retorno do volume de transações ETH
- eth_exchange_supply_diff: Diff do Supply em Exchanges (%) - Net Flow
- eth_social_dom_diff: Diff da Dominância Social do ETH
- eth_social_vol_log_ret: Log-retorno do Volume Social do ETH

MACRO (Indicadores Macroeconômicos):
------------------------------------
- spx_log_ret: Log-retorno do S&P 500
- dxy_log_ret: Log-retorno do DXY (US Dollar Index)
- nasdaq_log_ret: Log-retorno do NASDAQ
- gold_log_ret: Log-retorno do Ouro
- us10y_diff: Diff da taxa de juros US10Y (pontos base)
- vix_log_ret: Log-retorno do VIX (Índice de Medo)

MKCAP (Market Cap Metrics):
---------------------------
- total3_log_ret: Log-retorno do TOTAL3 (Altcoins excl. BTC/ETH/Stables)
- ssr_diff: Diff do SSR (Stablecoin Supply Ratio) - Poder de compra
- flippening_ratio_diff: Diff do Flippening Ratio (ETH/BTC)

SOCIAL (Sentimento de Mercado):
-------------------------------
- social_vol_log_spread: Spread log do volume social BTC vs Altcoins
- social_momentum: Diff do spread (aceleração do hype)
- total_market_noise_z: Z-Score do volume social total (euforia/apatia)
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
