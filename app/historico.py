import pandas as pd
import numpy as np
import MetaTrader5 as mt5
from datetime import datetime
import pytz

# Informações gerais para conexão
path = r'C:\Program Files\MetaTrader 5 Terminal\terminal64.exe'
login = 66304787
password =  'MT5@#!4500'
server = "XPMT5-DEMO"

# timezone
pytz.timezone('America/Sao_Paulo') #  fuso horario como utc
timing = datetime.now()


if not mt5.initialize(path=path,login=login,server=server,password=password): 
    print("initialize() failed, error code=",mt5.last_error())


def historico(ativo,size=365):

    """
    Retorna volatilidade realizada e preço maximo do ativo no período determinado
    Periodo = Padrão 365 dias (52 semanas) 
    Ativo: Ativo desejado
    size : Tamanho do dataframe, default 2000 linhas
    """
    from_date = timing # dia de hoje
    rates = mt5.copy_rates_from(ativo,mt5.TIMEFRAME_D1,from_date,size) # dados historicos do mt5
    rates_frames = pd.DataFrame(rates)
    rates_frames['time'] = pd.to_datetime(rates_frames['time'],unit='s')
    # Calculando volatilidade historica diária
    rates_frames['retorno'] = rates_frames['close'].pct_change()
    rates_frames.to_csv('{}_historico.csv'.format(ativo))
    

    
    
empresas = pd.read_excel("ibov-top40-volume.xlsx")
empresas.dropna(axis=1,inplace=True)


for e in empresas.values:
    mt5.market_book_add(e[0])
    try:
        historico(e[0])
    except Exception as e:
        pass
    
    
    
    
# Dados Financeiros

financeiros = pd.read_csv("dados_formatados")
financeiros = financeiros[['ATIVO', 'Data','Lucro Liquido','LPA','Receita Operacional Liquida','P/L','P/VPA','P/SALES','ROE']]


# Criando Indicadores Financeiros.

tickers = financeiros['ATIVO'].unique()

indicadores = {}
for i in tickers:
    
     # Medias 
     pl = round(financeiros['P/L'][financeiros['ATIVO'] == i].mean(),2)
     p_vpa = round(financeiros['P/VPA'][financeiros['ATIVO'] == i].mean(),2)
     p_sales = round(financeiros['P/SALES'][financeiros['ATIVO'] == i].mean(),2)
     
     # Atual vs Medias
     com_pl = round(((float(financeiros['P/L'][financeiros['ATIVO'] == i].tail(1))/pl) -  1)* 100,2) 
     com_vpa = round(((float(financeiros['P/VPA'][financeiros['ATIVO'] == i].tail(1))/p_vpa) -  1)* 100,2) 
     com_p_sales = round(((float(financeiros['P/SALES'][financeiros['ATIVO'] == i].tail(1))/p_sales) -  1)* 100,2) 
     

     indicadores[i] = {'P/L Médio':pl,
                       'P/VPA Médio':p_vpa,
                       'P/SALES Médio ':p_sales,
                       'P/L Atual vs Média':com_pl,
                       'P/VPA Atual vs Média':com_vpa,
                       'P/SALES Atual vs Média':com_p_sales}


indicadores = pd.DataFrame(indicadores).T
indicadores.to_csv('indicadores.csv')




# Dados por industria


outras_informacoes = pd.read_csv('dados_por_industria_2.csv',encoding='latin-1')
# floating e rounding os dados financeiros, para melhor apresentação.

for i in outras_informacoes:
    
    try:
        outras_informacoes[i].replace('-',np.nan,inplace=True)
        outras_informacoes[i] = outras_informacoes[i].astype(float).round(2)
    except:
        pass

    if "%" in i:
        outras_informacoes[i] = outras_informacoes[i] / 100

# Salvando os dados formatados.
outras_informacoes.to_csv('dados_por_industria.csv')

outras_informacoes

