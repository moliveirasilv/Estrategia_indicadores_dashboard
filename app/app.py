from numba.cuda.args import Out
from numba.cuda.simulator.cudadrv.devicearray import to_device
import plotly.express as px

import dash
from dash import dcc
from dash import html
from dash import dash_table

from plotly.subplots import make_subplots
import plotly.graph_objects as go
from dash.dependencies import Input, Output
from dash.dash_table.Format import Format, Group, Prefix, Scheme, Symbol # Localization
import urllib.parse

import pandas as pd
pd.options.mode.chained_assignment = None
import numpy as np
from datetime import datetime
import pytz #Modulo necessário para trabalhar com fuso horário

import functions as fc
import MetaTrader5 as mt5

from math import ceil
from matplotlib.cm import Set3


pytz.timezone('America/Sao_Paulo') #  fuso horario como utc
timing = datetime.now()

# Dados necessários:

# empresas mais negociadas do ibov
empresas = pd.read_excel('ibov-top40-volume.xlsx',header=None)
empresas.dropna(axis=1,inplace=True)

# datas de vencimento de opções (Necessário atualizar a cada de 6 meses para evitar errors)
vencimentos = pd.read_excel('vencimento_opcoes.xlsx')
vencimentos.dropna(axis=1,inplace=True)
vencimentos = vencimentos['VENCIMENTO']
to_drop = []
for i in range(0,len(vencimentos)):
    if (vencimentos.iloc[i] - timing).days < 0 or (vencimentos.iloc[i] - timing).days > 280:
        drop = i
        to_drop.append(drop)

vencimentos.drop(to_drop,axis=0,inplace=True)

# Dados Financeiros

financeiros = pd.read_csv("dados_formatados",thousands=r',')
financeiros = financeiros[['ATIVO', 'Data','Lucro Liquido','LPA','Receita Operacional Liquida','P/L','P/VPA','P/SALES','ROE']]

#Variacao dos principais indices
variacao = fc.variacao_indicadores('dados_formatados')


indicadores = pd.read_csv('indicadores.csv',thousands=r',')
indicadores.rename(columns={'Unnamed: 0':'Ativo'},inplace=True)

# Dados por industria

outras_informacoes = pd.read_csv('dados_por_industria.csv',encoding='utf-8')
comparaveis = outras_informacoes.copy()



# colunas da tabela de opções

tbcols_formated = Format()
tbcols_formated_2 = Format()
precision = Format()
formated_price = tbcols_formated.scheme(Scheme.fixed).precision(2).symbol(Symbol.yes).symbol_prefix('R$ ')
formated_percentage = tbcols_formated_2.scheme(Scheme.percentage).precision(2)
precision_formated = precision.precision(2)

tblcols = [{'name':'Ticker','id':'ticker'},
           {'name':'Ultimo','id':'ultimo','type':'numeric','format':formated_price},
           {'name':'Preço Ativo','id':'Ativo Subjacente','type':'numeric','format':formated_price},
           {'name':'Strike','id':'Strike','type':'numeric','format':formated_price},
           {'name':'Volume','id':'Volume'},
           {'name':'N° Negocios','id':'N° de Negocios'},
           {'name':'Vencimento','id':'Vencimento em(dias)'},
           {'name':'Preço Teorico','id':'Preco Teorico','type':'numeric','format':formated_price},
           {'name':'Vol Implicita','id':'Implicita','type':'numeric','format':formated_percentage},
           {'name':'Atual x Max ','id':'Distancia do Max Historico(%)','type':'numeric','format':formated_percentage},
           {'name':'Vol Hist x Vol Implicita','id':'Diferença entre Implicita e Realizada(%)','type':'numeric','format':formated_percentage},
           {'name':'Delta','id':'Delta'}]

# Financeiros

formatted = Format()
formatted = formatted.scheme(Scheme.fixed).precision(2).symbol(Symbol.yes).symbol_prefix('R$ ')

tbcols_financeiros = [{'name':'Ativo','id':'ATIVO'},
                      {'name':'Data','id':'Data'},
                      {'name':'Lucro Líquido','id':'Lucro Liquido','type':'numeric','format':formatted},
                      {'name':'LPA','id':'LPA','type':'numeric','format':formatted},
                      {'name':'Receita Operacional Líquida','id':'Receita Operacional Liquida','type':'numeric','format':formatted},
                      {'name':'P/L','id':'P/L'},
                      {'name':'P/VPA','id':'P/VPA'},
                      {'name':'P/SALES','id':'P/SALES'},
                      {'name':'ROE','id':'ROE'}]


# Colunas de Multiplos
tblcols_2 = [{'name':'Código','id':'Código'},
             {'name':'Setor Bovespa','id':'Subsetor Bovespa','hideable':True},
             {'name':'P/VPA','id':'P / VPA|||em vezes'},
             {'name':'P/L','id':'Preço / Lucro|12 meses||em vezes'},
             {'name':'P/SALES','id':'Preço / Vendas|12 meses||em vezes'},
             {'name':'ROE','id':'ROE|12 meses||em %','type':'numeric','format':precision_formated}
             ]

# Colunas dos Retornos
tblcols_3 = [{'name':'Código','id':'Código'},
            #  {'name':'Último Fechamento vs Max 52','id':'Fechamento vs|Máx 52 sem||em %','type':'numeric','format':formated_percentage},
             {'name':'Max 52','id':'Max 52s','type':'numeric','format':formated_price},
             {'name':'Retorno 1d','id':'Retorno 1d','type':'numeric','format':formated_percentage},
             {'name':'Retorno 1 mês','id':'Retorno 30d','type':'numeric','format':formated_percentage},
             {'name':'Retorno 12 meses','id':'Retorno 52s','type':'numeric','format':formated_percentage},
             {'name':'Volatilidade','id':'Volatilidade 52s','type':'numeric','format':formated_percentage},
             #{'name':'Beta','id':'Beta|60 meses|vs IBOV'}
             ]

# Colunas das Posições

tblcols_4 = [{'name':'Call 1','id':'Call 1'},
             {'name':'Call 2','id':'Call 2'},
             {'name':'Vencimento','id':'Vencimento'},
             {'name':'Preço Teórico(Call 1)','id':'Preço Teórico(Call 1)','type':'numeric','format':formated_price},
             {'name':'Preço Teórico(Call 2)','id':'Preço Teórico(Call 2)','type':'numeric','format':formated_price},
             {'name':'Diferença entre Preços','id':'Diferença entre Preços','type':'numeric','format':formated_price},
             {'name':'Strike Call 1','id':'Strike Call 1','type':'numeric','format':formated_price},
             {'name':'Strike Call 2','id':'Strike Call 2','type':'numeric','format':formated_price},
             {'name':'Diferença entre strikes','id':'Diferença entre strikes','type':'numeric','format':formated_price},
             {'name':'Ganho Máximo','id':'Ganho Máximo','type':'numeric','format':formated_percentage},
             {'name':'Queda para Prejuízo','id':'Queda para Prejuízo','type':'numeric','format':formated_percentage},
             {'name':'Preco Breakeven','id':'Preco Breakeven','type':'numeric','format':formated_price},
             {'name':'Delta Call 1','id':'Delta Call 1'},
             {'name':'Delta Call 2','id':'Delta Call 2'},
            #  {'name':'Vol Implicita(Call 1)','id':'Vol Implicita(Call 1)','type':'numeric','format':formated_percentage},
            #  {'name':'Vol Implicita(Call 2)','id':'Vol Implicita(Call 2)','type':'numeric','format':formated_percentage},
             {'name':'Implicita vs Histórica(Call 1)','id':'Implicita vs Histórica(Call 1)','type':'numeric','format':formated_percentage},
             {'name':'Implicita vs Histórica(Call 2)','id':'Implicita vs Histórica(Call 2)','type':'numeric','format':formated_percentage}]

#COLOR AND FONT DEFINITION
grey = '#e0e1f5'
black = '#212121'
fontsize = 18
fontfamily = 'Arial, sans-serif'



# Versão inicial dash


app = dash.Dash(__name__) ## start app

app.layout = html.Div(children=[
    

    html.Div(children=[
    dcc.Dropdown(id='empresas_dropdown',
                    options=[{'label':str(*row),'value':str(*row)} for row in empresas.values],
                    value='PETR4',
                    style={'display':'block'}
    )],
    style={'display': 'block', 'vertical-align': 'top'}
    ),
    
    html.Br(),
    
    html.Div(children=[
    html.P(html.B('Datas de Vencimento:')),
        dcc.Checklist(id= 'datas-vencimento',
        options = [{'label':i.strftime("%Y-%m-%d"),'value':i.strftime("%Y-%m-%d")} for i in vencimentos],
        # labelStyle={'display':'inline-block'},
        value = [vencimentos.iloc[0].strftime("%Y-%m-%d")]
        )],
    style={'display': 'block', 'vertical-align': 'top'}
             ),

    dcc.Tabs([
        
        dcc.Tab(label='Opções', children =[ 
        html.H4('Opções: Covered Calls'),
        
        html.Br(),
        
        
        html.Br(),
        # Tabela 
        html.A(
        'Download Dados',
        id='download_dados',
        download="opcoes.csv",
        href="",
        target="_blank"
        ),
        html.Br(),
        dash_table.DataTable(id='table',
                            data=fc.streaming('PETR4',[vencimentos.iloc[0].strftime("%Y-%m-%d")]),
                            columns=tblcols,
                            #filter_action="native",
                            sort_action="native",
                            sort_mode='multi',
                            page_action='native',
                            page_current= 0,
                            page_size= 30,
                            style_table={'height': '500px', 'overflowY': 'auto'},
                            style_cell = {'font-family': fontfamily,'fontSize': fontsize},
                            style_header={
                                        'overflow': 'hidden',
                                        'textOverflow': 'ellipsis',
                                        'width':250,
                                        'minWidth':200,
                                        'maxWidth': 300}),
        
        dcc.Interval(id='interval',interval=10000,n_intervals=0), # componente necessário para atualização automatica
        
        html.Br()
        
        ]),
        
        dcc.Tab(label='Indicadores Financeiros', children=[
            html.Br(),
            
            html.P(html.B('Indicadores Financeiros:')),
            
            html.Br(),
            
            # Tabela com os retornos (Talvez seja melhor fazer esses calculos com metatrader5),
            # No momento essas informaçõe não estão atualizando diáriamente
            # Portanto acredito que o melhor é fazer a parte dos retornos no beta trader.
            dash_table.DataTable(id='table-retornos',
                                 columns=tblcols_3,
                                 data= fc.retornos_volatilidade('PETR4')[0],
                                 sort_action="native",
                                 sort_mode='multi',
                                 page_action='native',
                                 style_cell = {'font-family': fontfamily,'fontSize': fontsize}
                                 ),
            
            html.Br(),
            
            html.P(html.B('Comparáveis(Subsetor Bovespa)')),
            
            dash_table.DataTable(id='table-retornos-comparaveis',
                            columns=tblcols_3,
                            data= fc.retornos_volatilidade('PETR4')[1],
                            sort_action="native",
                            sort_mode='multi',
                            page_action='native',
                            style_cell = {'font-family': fontfamily,'fontSize': fontsize}
                            
                            ),
            
            html.Br(),
            # Indicadores históricos
            dash_table.DataTable(id='table-financeiro',
                                 columns=tbcols_financeiros,
                                 data=financeiros[financeiros['ATIVO'] == 'PETR4'].to_dict('records'),
                                 sort_action="native",
                                 sort_mode='multi',
                                 page_action='native',
                                 style_cell = {'font-family': fontfamily,'fontSize': fontsize}),
            html.Br(),
            
            html.Div(dcc.Graph(id='variacao_indicadores')),
            
            
            html.Br(),
                        
            # Indicadores da empresa
            dash_table.DataTable(id='table-indicadores',
                                 columns = [{'name':i, 'id':i} for i in indicadores],
                                 data=indicadores[indicadores['Ativo']=='PETR4'].to_dict('records'),
                                 style_cell = {'font-family': fontfamily,'fontSize': fontsize}),
            
        
            html.Br(),
            
            html.P(html.B('Multiplos de empresas comparáveis')),
            dash_table.DataTable(id='table-comparaveis',
                                columns = tblcols_2,
                                data=comparaveis[(comparaveis['Subsetor Bovespa'].str.contains('Petróleo gás e biocombustíveis')) 
                                                 & ~(comparaveis['Código'].str.contains('PETR'))].to_dict('records'),
                                sort_action="native",
                                sort_mode='multi',
                                page_action='native',
                                style_cell = {'font-family': fontfamily,'fontSize': fontsize}),
            

                        
            
            
            
            html.Br(),
            
        

        ]),
        dcc.Tab(label='Posições',children=[
            
            html.P(html.B('Possíveis posições de Bull Spread Calls')),
            html.A(
            'Download Dados',
            id='download_dados_posicao',
            download="opcoes_posicao.csv",
            href="",
            target="_blank"
            ),
            html.Br(),
            
            dash_table.DataTable(id='table-posicoes',
                                 data= fc.posicoes_montandas('PETR4',[vencimentos.iloc[0].strftime("%Y-%m-%d")]),
                                 columns=tblcols_4,
                                 sort_action="native",
                                 sort_mode='multi',
                                #fixed_rows={'headers': True},
                                 page_action='native',
                                 page_current= 0,
                                 page_size= 30,
                                 style_table={'height': '500px','overflowY': 'auto'},
                                 style_cell = {'font-family': fontfamily,'fontSize': fontsize},
                                 style_header={
                                                'overflow': 'hidden',
                                                'textOverflow': 'ellipsis',
                                                'width':250,
                                                'minWidth':200,
                                                'maxWidth': 300}
                                 ),
            
            dcc.Interval(id='interval-posicoes',interval=10000,n_intervals=0),
            html.Br(),
            html.Br(),
            html.Div(children=[
            dcc.Input(id='call_comprada',
                    type = 'text',
                    placeholder='Ticker da Call ITM'
                        )], 
                    style={'display': 'inline-block'}),
            
            html.Div(children=[
            dcc.Input(id='call_vendida',
                    type='text',
                    placeholder='Ticker da Call ATM'
            )
            ],
            style={'display': 'inline-block'}),
            html.Br(),
            html.Div(dcc.Graph(id='graph'))
            
        ])
    
    ])
         ]) # html layout of the app


@app.callback(Output('table','data'),
              Input('empresas_dropdown','value'),
              Input('datas-vencimento','value'),
              Input('interval','n_intervals')
              )

def updateTable(input_ativo,input_vencimento,n):
     return fc.streaming(input_ativo,input_vencimento)

@app.callback(
    Output("download_dados", "href"),
    Input('empresas_dropdown','value'),
    Input('datas-vencimento','value'),
)
def download_button(input_ativo,input_vencimento):
    
    df = pd.DataFrame(fc.streaming(input_ativo,input_vencimento))
    csv_dados = df.to_csv(index=False, encoding='latin-1')
    csv_dados = "data:text/csv;charset=latin-1," + urllib.parse.quote(csv_dados)
    return csv_dados



@app.callback(Output('table-posicoes','data'),
              Input('empresas_dropdown','value'),
              Input('datas-vencimento','value'),
              Input('interval','n_intervals'))

def updatePosicoes(input_ativo,input_vencimento,n):
    return fc.posicoes_montandas(input_ativo,input_vencimento)

@app.callback(
    Output("download_dados_posicao", "href"),
    Input('empresas_dropdown','value'),
    Input('datas-vencimento','value'),
)
def download_button_posicao(input_ativo,input_vencimento):
    
    df = pd.DataFrame(fc.posicoes_montandas(input_ativo,input_vencimento))
    csv_dados = df.to_csv(index=False, encoding='latin-1')
    csv_dados = "data:text/csv;charset=latin-1," + urllib.parse.quote(csv_dados)
    return csv_dados

@app.callback(Output('table-financeiro','data'),
              Input('empresas_dropdown','value'))

def updateTable_financeiro(input_ativo):
    return financeiros[financeiros['ATIVO'] == '{}'.format(input_ativo)].to_dict('records')


@app.callback(Output('table-indicadores','data'),
              Input('empresas_dropdown','value'))

def updateTable_financeiro(input_ativo):
    return indicadores[indicadores['Ativo'] == '{}'.format(input_ativo)].to_dict('records')

@app.callback(Output('table-comparaveis','data'),
              Input('empresas_dropdown','value'))

def updateComparaveis(input_ativo):
    setores = str(*comparaveis['Subsetor Bovespa'][comparaveis['Código'] == '{}'.format(input_ativo)])
    return comparaveis[(~comparaveis['Código'].str.contains('{}'.format(str(input_ativo).rstrip('123456789')))) & (comparaveis['Subsetor Bovespa'] == setores)].to_dict("records")

@app.callback(Output('table-retornos','data'),
              Input('empresas_dropdown','value'),
              )
def retornos(input_ativo):
    return fc.retornos_volatilidade(input_ativo)[0]


@app.callback(Output('table-retornos-comparaveis','data'),
              Input('empresas_dropdown','value'),
              )

def retornos_comparaveis(input_ativo):
    return fc.retornos_volatilidade(input_ativo)[1]

@app.callback(
    Output(component_id='graph',component_property='figure'),
    [Input(component_id='empresas_dropdown',component_property='value'),
     Input(component_id='call_comprada',component_property='value'),
     Input(component_id='call_vendida',component_property='value')]
)

def display_selected_call(ativo,call_1, call_2):
    
    if (call_1 or call_2) is None:
        return {}
    else:
        fig = fc.option_figure(ativo,call_1,call_2)
        return fig


@app.callback(Output('variacao_indicadores','figure'),
             Input('empresas_dropdown','value'))

def variacao_figure(input_ativo):
    
    ativo = variacao[input_ativo]

    fig = make_subplots(
    rows=2, cols=2,
    subplot_titles=("Variação P/L", "Variação P/VPA", "Variação P/SALES", "Variação LPA"))

    fig.add_trace(go.Scatter(x=ativo['Data'],y=ativo['Variacao P/L']),
                row=1, col=1)

    fig.add_trace(go.Scatter(x=ativo['Data'],y=ativo['Variacao P/VPA']),
                row=1, col=2)

    fig.add_trace(go.Scatter(x=ativo['Data'],y=ativo['Variacao P/SALES']),
                row=2, col=1)

    fig.add_trace(go.Scatter(x=ativo['Data'],y=ativo['Variacao LPA']),
                row=2, col=2)

    fig.update_layout(height=800, width=1200,showlegend=False)

    return fig

if __name__ == '__main__':
    app.run_server(debug=True)

