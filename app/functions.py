from sys import meta_path
import pandas as pd
import numpy as np
from datetime import datetime
import pytz #Modulo necessário para trabalhar com fuso horário
import time
import MetaTrader5 as mt5
import plotly.express as px
from collections import OrderedDict
from workadays import workdays as wd
  # default='warn'


from numpy.core.fromnumeric import var
from py_vollib.black.implied_volatility import implied_volatility
from py_vollib.black.implied_volatility import implied_volatility_of_discounted_option_price as ivdp
from py_vollib.black_scholes import black_scholes
from py_vollib.black_scholes_merton import black_scholes_merton
from py_vollib.black.greeks.analytical import *




# Inicializando mt5

path = r'C:\Program Files\MetaTrader 5 Terminal\terminal64.exe' # path do terminal 
login = 66304787
password =  'MT5@#!4500'
server = "XPMT5-DEMO"

# timezone
pytz.timezone('America/Sao_Paulo') #  fuso horario como utc
timing = datetime.now()

# Inicializar a conexão
if not mt5.initialize(path=path,login=login,server=server,password=password): 
    print("initialize() failed, error code=",mt5.last_error())
    mt5.shutdown()
    
#ativos e similares
ativo_e_similar = {}


def vol_e_preco_max(ativo):
    
    """
    Retorna preço e volatilidade realizada do ativo
    OBS: MUDAR O PATH 
    """
    rates_frames = pd.read_csv('Dados Históricos\{}_historico.csv'.format(ativo))
    vol = rates_frames['retorno'].std() * 252 ** (1/2) # volatilidade realizada anualizada
    preco_max = rates_frames['close'].max() # maxima historica

    return vol, preco_max


def call_negociadas(ativo, data_do_vencimento=[]):
    
    """
    Retorna tickers de todas as calls negociadas com vencimento até 160 dias.
    Ativo: nome do ativo sem o número de on ou pn, exemplo: Se quiser calls de PETROBRAS, o input deve ser "PETR"
    
    """
    ativo = ativo.rstrip('123456789')
    calls_codigos = "ABCDEFGHIJKLN"
    # Pegar o ticks de todas as opçoes negociadas.
    calls_names = []
    nomes_calls = []
    for codigo in calls_codigos:
        calls_name = "*{}".format(ativo)+"{}*".format(codigo)
        calls_names.append(calls_name)
    for calls in calls_names:
        data = mt5.symbols_get(calls)
        for s in data:
            expiration_date = datetime.fromtimestamp(s.expiration_time).strftime("%Y-%m-%d") # 
            string_date = str(expiration_date)
            # now_to_expired_days = (expiration_date - TIME_NOW).days        
            
            if string_date in data_do_vencimento:
                #print(s.name)
                nomes_calls.append(s.name)
    return nomes_calls

def streaming(input_ativo,input_data=[]):
    
    """
    
    Retorna os dados de opções do ativo em real(se o mercado estiver aberto)
    input_ativo: ativo desejado
    input_data: Datas de vencimento das opções
    
    
    """
    
    vol, preco_max = vol_e_preco_max(ativo=input_ativo)
    dados = call_negociadas(ativo=input_ativo, data_do_vencimento=input_data)
    # time.sleep(30) # garantir que a conexão foi garantida antes de a chamar a função de streaming
    
    mt5.market_book_add(input_ativo)
    for ticker in dados:
        mt5.market_book_add(ticker)
        
    mt5.market_book_add('DI1@')


    while(True):
    
        df = pd.DataFrame()
        last_subjacente = float(mt5.symbol_info_tick(input_ativo).last) # ultimos preço do ativo subjacente
        
        for s in dados:
            
            # informações do ativo
            name = s  #  nome do ativo
            simbol =  mt5.symbol_info(s)
            last = float(simbol.last) # ultimo preço da call
            strike = float(mt5.symbol_info(s).option_strike) # strike
            expiration_date =  wd.networkdays(timing,(datetime.fromtimestamp(simbol.expiration_time)),country='BR',state='SP') / 365
            volume = float(simbol.session_volume)
            n_negocios = float(simbol.session_deals)
            di = mt5.symbol_info_tick('DI1@').last / 100  # di
            
            # calculo
            preco_teorico = round(black_scholes_merton('c',last_subjacente,strike,expiration_date,di,vol,q=0),2)
            vol_implicita = implied_volatility(preco_teorico,last_subjacente,strike,expiration_date,di,'c')
                
            
            dl = delta('c',last_subjacente,strike,expiration_date,di,vol)
            
            distancia_preco_max  = (preco_max / last_subjacente - 1)
            diferenças_volatilidades = ( vol_implicita - vol )
                
            
            lista = {'ticker':name,
                    'ultimo':round(last,2),
                    'Ativo Subjacente':last_subjacente,
                    'Strike':strike,
                    'Volume':volume,
                    'N° de Negocios': n_negocios,
                    'Vencimento em(dias)':round((expiration_date*365)),
                    'Preco Teorico':preco_teorico,
                    'Implicita':vol_implicita,
                    'Distancia do Max Historico(%)': distancia_preco_max,
                    'Diferença entre Implicita e Realizada(%)': diferenças_volatilidades,
                    'Delta':round(dl,4)}
            
            df = df.append(lista,ignore_index=True)
        tabela = df
        time.sleep(0.5)
        return tabela.to_dict('records')


def payoffs(preco, strike, preco_ativo, tipo='c'):
    
    """
    Retorna os payoffs da call, dado o preço da call, strike, e último preço do ativo subjacente e
    se a opção foi comprada o vendida.
    """
    
    
    ## Gera uma array com preços do ativo subjacente baseado no preço do momento do ativo.  
    p_min, p_max = int(preco_ativo * 0.70), int(preco_ativo * 1.2)
    step = (p_max - p_min) * 100
    ativo_subjacente = np.round(np.linspace(p_min,p_max,step), decimals=2)
    
    
    payoffs = []
    for price in ativo_subjacente:

        if tipo == 'v':

            if price > strike:
                payoff = - (price - strike - preco)
                payoffs.append(round(payoff, 2))        
            else:
                payoff = preco
                payoffs.append(round(payoff, 2))
                
        elif tipo == 'c':
        
            if price > strike:
                payoff = price - strike - preco
                payoffs.append(round(payoff, 2))        
            else:
                payoff = - preco
                payoffs.append(round(payoff, 2))
    
    payoffs = pd.DataFrame({'AtivoSubjacente': ativo_subjacente,
                                    'Payoffs': payoffs})
    
    return payoffs
    


def posicoes_montandas(input_ativo, input_data=[]):
    
    while True:
        df = streaming(input_ativo,input_data)
        df = pd.DataFrame(df)

        itm = df[(df['Delta'] > 0.85) & (df['Delta'] <= 1.)].copy()
        atm = df[(df['Delta'] > 0.50) & (df['Delta'] <= 0.75)].copy()

        dataframe = list()
        ativo = itm['Ativo Subjacente'].iloc[0]
        
        for i in itm.index:
                    
            if itm['Preco Teorico'].loc[i] != 0:

                preco_comprado = itm['Preco Teorico'].loc[i].item()
                strike_comprado = itm['Strike'].loc[i].item()
                vencimento = itm['Vencimento em(dias)'].loc[i].item()
                tickers_c = itm['ticker'].loc[i]

                delta_comprado = itm['Delta'].loc[i].item()
                vol_implicita_c = itm['Implicita'].loc[i].item()
                implicita_historica_c = itm['Diferença entre Implicita e Realizada(%)'].loc[i].item()

                for j in atm.index:
                    if atm['Preco Teorico'].loc[j].item() != 0 and atm['Vencimento em(dias)'].loc[j].item() == vencimento:

                        preco_vendido = atm['Preco Teorico'].loc[j]
                        strike_vendido = atm['Strike'].loc[j]
                        try:
                            tickers_v = atm['ticker'].loc[j]
                        except:
                            tickers_v = np.nan
                        
                        delta_vendido = atm['Delta'].loc[j].item()
                        vol_implicita_v = atm['Implicita'].loc[j].item()
                        implicita_historica_v = atm['Diferença entre Implicita e Realizada(%)'].loc[j].item()


                        payoff_comprada = payoffs(preco_comprado,strike_comprado,ativo,tipo='c')
                        payoff_vendida = payoffs(preco_vendido,strike_vendido,ativo,tipo='v')

                        payoff_operacao = pd.merge(payoff_comprada, payoff_vendida, on='AtivoSubjacente', suffixes=('_comprado', '_vendido'))
                        payoff_operacao['Payoff_total'] = payoff_operacao.Payoffs_comprado + payoff_operacao.Payoffs_vendido

                        
                        try:
                            breakeven = payoff_operacao[payoff_operacao.Payoff_total == 0]
                            preco_breakeven = breakeven.AtivoSubjacente.item()
                            precos_diferenca = preco_comprado - preco_vendido
                            strike_diferenca = strike_comprado - strike_vendido
                            queda_para_prejuizo = (breakeven.AtivoSubjacente.item()/ativo - 1)
                            ganho_maximo = ((preco_comprado - preco_vendido + payoff_operacao.Payoff_total.iloc[-1]) / (preco_comprado - preco_vendido) - 1)
                        except ValueError:
                            breakeven = np.nan
                            preco_breakeven = np.nan
                            ganho_maximo = np.nan
                            strike_diferenca = np.nan
                            queda_para_prejuizo = np.nan
                            pass
                        
                    dados = OrderedDict([('Call 1',tickers_c),
                                         ('Call 2',tickers_v),
                                         ('Vencimento',vencimento),
                                         ('Preço Teórico(Call 1)',preco_comprado),
                                         ('Preço Teórico(Call 2)',preco_vendido),
                                         ('Diferença entre Preços',precos_diferenca),
                                         ('Strike Call 1',strike_comprado),
                                         ('Strike Call 2',strike_vendido),
                                         ('Diferença entre strikes',strike_diferenca),
                                         ('Ganho Máximo',ganho_maximo),
                                         ('Queda para Prejuízo',queda_para_prejuizo),
                                         ('Preco Breakeven',preco_breakeven),
                                         ('Delta Call 1',delta_comprado),
                                         ('Delta Call 2',delta_vendido),
                                         ('Vol Implicita(Call 1)',vol_implicita_c),
                                         ('Vol Implicita(Call 2)',vol_implicita_v),
                                         ('Implicita vs Histórica(Call 1)',implicita_historica_c),
                                         ('Implicita vs Histórica(Call 2)',implicita_historica_v),
                                        ])
                    dataframe.append(dados)
        df = pd.DataFrame(dataframe)
        return df.to_dict('records')           
        
        
        
        


def option_figure(ativo_sub,call_1,call_2):
    
    "Retorna o grafico de payoff das opções selecionadas."
    
    preco_itm, strike_itm = float(mt5.symbol_info_tick(call_1).last), float(mt5.symbol_info(call_1).option_strike)
    preco_atm, strike_atm = float(mt5.symbol_info_tick(call_2).last), float(mt5.symbol_info(call_2).option_strike) 
    ativo = float(mt5.symbol_info_tick(ativo_sub).last)
    
    payoff_comprada = payoffs(preco_itm,strike_itm,ativo,tipo='c')
    payoff_vendida = payoffs(preco_atm,strike_atm,ativo,tipo='v')
    
    payoff_operacao = pd.merge(payoff_comprada, payoff_vendida, on='AtivoSubjacente', suffixes=('_comprado', '_vendido'))
    payoff_operacao['Payoff_total'] = payoff_operacao.Payoffs_comprado + payoff_operacao.Payoffs_vendido
    breakeven = payoff_operacao.loc[payoff_operacao.Payoff_total == 0]
    
    
    #Figure
    fig = px.line(
        title="Payoff Opções",
        x=payoff_operacao.AtivoSubjacente, 
        y=payoff_operacao.Payoff_total,
        labels={"x": "Preço do Ativo",  "y": "Payoff"},
       )

    fig.add_annotation(x=ativo, y=payoff_operacao.loc[payoff_operacao.AtivoSubjacente == ativo, 'Payoff_total'].item(),
                text="Resultado atual",
                showarrow=True,
                arrowhead=2)

    fig.update_yaxes(zeroline=True, zerolinecolor="#FF0000", spikedash='dot')

    fig.update_layout(hovermode="x")

    return fig



def variacao_indicadores(dados):
    '''
    Retorna o dicionario contendo a variação dos principais indicadores para cada ativo na base de dados(arquivo referencia
    ibov-top40-volume.xlsx)
    '''
    
    financeiros = pd.read_csv(dados,thousands=r',')

    variacoes = {}
    for i in financeiros['ATIVO'].unique():

        data = financeiros[financeiros['ATIVO'] == i]['Data'].reset_index(drop=True)
        var_lpa = financeiros['LPA'][financeiros['ATIVO'] ==  i].pct_change().reset_index(drop=True)
        var_pl = financeiros['P/L'][financeiros['ATIVO'] ==  i].pct_change().reset_index(drop=True)
        var_pvpa = financeiros['P/VPA'][financeiros['ATIVO'] ==  i].pct_change().reset_index(drop=True)
        var_psales = financeiros['P/SALES'][financeiros['ATIVO'] ==  i].pct_change().reset_index(drop=True)
        ativo = [i]*len(var_pl)

        variacao =     {'ATIVO':ativo,
                        'Data':data,
                        'Variacao LPA':var_lpa,
                        'Variacao P/L':var_pl,
                        'Variacao P/VPA':var_pvpa,
                        'Variacao P/SALES': var_psales}

        variacoes[i] = variacao

    return variacoes


def retornos_volatilidade(input_ativo):
    
    dia_hoje = timing.strftime('%Y-%m-%d')
    setores = pd.read_excel('setores.xlsx').dropna(axis=1)
    historico = pd.DataFrame(mt5.copy_rates_from_pos(input_ativo,mt5.TIMEFRAME_D1,0,252))
    historico.index = pd.to_datetime(historico['time'],unit='s')
    historico.sort_index(ascending=False,inplace=True)

    
    ativo_setor = setores['Subsetor Bovespa'][setores['Código'] == input_ativo].item()
    ativo_g = input_ativo.rstrip('123456789')
    ativos_similares = [i for i in setores['Código'][(setores['Subsetor Bovespa'] == ativo_setor) & (~setores['Código'].str.contains(ativo_g))]]
    
    max_52s = historico['close'].max() # 52 semanas
    retorno_52s = historico['close'][0]/historico['close'][-1] - 1 # retorno 52 semanas
    vol_52s = historico['close'].pct_change().std() * 252 ** (1/2) # volatilidade de 52 semanas
    retorno_30d = historico['close'].head(21)[0]/historico['close'].head(21)[-1] - 1 # retorno 1 mês
    retorno_1d = historico['close'][0]/historico['close'][1] - 1 # retorno dia
    
    ativo_e_similar[input_ativo+'_'+dia_hoje]= {'Código':input_ativo,
                             'Data':dia_hoje,
                             'Max 52s':max_52s,
                             'Retorno 52s':retorno_52s,
                             'Volatilidade 52s':vol_52s,
                             'Retorno 30d':retorno_30d,
                             'Retorno 1d':retorno_1d}
    
    
    
    
    for ativo in ativos_similares:
        
        name = ativo +'_'+ dia_hoje
    
        if name not in ativo_e_similar.keys():

            historico = pd.DataFrame(mt5.copy_rates_from_pos(ativo,mt5.TIMEFRAME_D1,0,252))
            historico.index = pd.to_datetime(historico['time'],unit='s')
            historico.sort_index(ascending=False,inplace=True)
            
            max_52s = historico['close'].max() # 52 semanas
            retorno_52s = historico['close'][0]/historico['close'][-1] - 1 # retorno 52 semanas
            vol_52s = historico['close'].pct_change().std() * 252 ** (1/2) # volatilidade de 52 semanas
            retorno_30d = historico['close'].head(21)[0]/historico['close'].head(21)[-1] - 1 # retorno 1 mês
            retorno_1d = historico['close'][0]/historico['close'][1] - 1 # retorno dia

            ativo_e_similar[name]= {
                                    'Código':ativo,
                                    'Max 52s':max_52s,
                                    'Data': dia_hoje,
                                    'Retorno 52s':retorno_52s,
                                    'Volatilidade 52s':vol_52s,
                                    'Retorno 30d':retorno_30d,
                                    'Retorno 1d':retorno_1d}
            
    #novos_similares = [i+'_'+dia_hoje for i in ativos_similares]
    retornos = pd.DataFrame([ativo_e_similar.get(key) for key in ativo_e_similar.keys()])
    ativo = retornos[retornos['Código']==input_ativo].to_dict('records')
    similares = retornos[retornos['Código'].isin(ativos_similares)].to_dict('records')
    return ativo, similares