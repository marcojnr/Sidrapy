import pandas as pd
import numpy as np
from datetime import date, datetime
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import sidrapy
import pyodbc

#COLETANDO TABELAS DO IBGE COM O SIDRAPY
#help(sidrapy.table)

def transform_data(tabela):
    #Filtrar
    tabela = tabela[(tabela.D1C > '3300000') & (tabela.D1C < '3400000')]

    tabela = tabela[['D1C', 'D1N', 'D2N', 'V']]

    #renomear colunas
    tabela = tabela.rename(columns={'D1C': 'IDGeografico', 'D1N':'Municipio',
                                            'D2N': 'AnoReferencia', 'V': 'Valor'})
    #mudar tipagem
    tabela["IDGeografico"] = tabela["IDGeografico"].astype(int)
    tabela["Valor"] = tabela["Valor"].astype(float)
                
    #Remover o municipio do RJ
    # i = tabela[((tabela.CodigoGeografico == 3304557))].index
    # tabela = tabela.drop(i)

    return pd.DataFrame(tabela)


# - População Estimada
populacaoE = sidrapy.get_table(table_code= "6579",
                              variable= "9324",
                              territorial_level= '6',
                              ibge_territorial_code= "all",
                              period= "last",
                              header= "n")


tbl_Populacao = transform_data(populacaoE)



# - PIB
pib = sidrapy.get_table(table_code= "5938",
                              variable= "37",
                              territorial_level= "6",
                              ibge_territorial_code= "all",
                              period= "last",
                              header= "n")

tbl_pib = transform_data(pib)


# - Esgotamento Sanitario

esgotamentoS = sidrapy.get_table(table_code= "1394",
                       variable= "1000096",
                       classification= "11558",
                       categories= "92855",
                       territorial_level= "6",      
                       ibge_territorial_code= "all",    
                       period= "last",
                       header= "n")

tbl_esgotamentoS = transform_data(esgotamentoS)


# Juntar os dataframes
tabelaIBGE = pd.merge(tbl_Populacao, tbl_pib, how='inner', on=["IDGeografico","Municipio"])

tabelaIBGE = pd.merge(tbl_esgotamentoS, tabelaIBGE, how='left', 
                       on=["IDGeografico","Municipio"])
                       

# renomear colunas
tabelaIBGE = tabelaIBGE.rename(columns={'AnoReferencia':'PeriodoEsgotamentoSanitario', 'IDGeografico': 'Cod_IBGE',
                                        'Valor': 'EsgotamentoSanitario', 'AnoReferencia_x':'PeriodoPopulacaoEstimada',
                                          'Valor_x': 'PopulacaoEstimada', 'AnoReferencia_y': 'PeriodoPIB',
                                          'Valor_y': 'PIB'})


ano_atual = date.today().year
tabelaIBGE['Ano'] = ano_atual

tabelaIBGE['DataCarga'] = datetime.now().replace(microsecond = 0)

tabelaIBGE['ProcessoCarga'] = 'CensoIBGEPopulação'


# conexao com o banco 
str_conexao = 'DRIVER={SQL Server};SERVER=DESKTOP-EA0EN72;DATABASE=IBGE;Trusted_Connection=yes'
conexao = pyodbc.connect(str_conexao)
print(conexao.execute('SELECT @@VERSION').fetchone()[0])

#Pegar tabela DimEnte 
DimMunicipio = pd.read_sql_query('SELECT * FROM dbo.Municipios_RJ', conexao)
print(DimMunicipio)


#Pegar tabela DimIBGEHistorico 
DimIBGEHistorico = pd.read_sql_query('SELECT * FROM dbo.IBGEHistorico', conexao)
print(DimIBGEHistorico)

# # precisa encerrar conexao
conexao.close()

#merge da tabelaIBGE com DimMunicipio para pegar o ID
tabelaIBGE = pd.merge(tabelaIBGE, DimMunicipio[['ID_Municipio','Cod_IBGE']], 
                        how='left', on=["Cod_IBGE"])

#Ordenar colunas
tabelaIBGE = tabelaIBGE[['ID_Municipio', 'Ano', 'Cod_IBGE', 'Municipio', 'PopulacaoEstimada','PeriodoPopulacaoEstimada',
                            'PIB','PeriodoPIB','EsgotamentoSanitario', 'PeriodoEsgotamentoSanitario','ProcessoCarga','DataCarga']]


#Apagar colunas
tabelaIBGE.drop(['Cod_IBGE', 'Municipio'], inplace=True, axis=1)


print(tabelaIBGE.head(10))

# tabelaIBGE.to_excel("teste.xlsx")

mes_atual = tabelaIBGE['DataCarga'][0].month

data_recente = max(DimIBGEHistorico['DataCarga'])
print(data_recente)

mes_IBGEHist = data_recente.month


engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % quote_plus(str_conexao))
print(engine.execute('SELECT @@VERSION').fetchone()[0])

with engine.begin() as conn:
    if tabelaIBGE.Ano[0] > max(DimIBGEHistorico.Ano): 
        # INSERT
        tabelaIBGE.to_sql(name="DimIBGEHistorico", con=engine, schema='dbo', if_exists="append", index=True, index_label= 'IdIBGE')
        
        print("Novos registros foram inseridos na tabela DimIBGEHistorico referente aos dados do IBGE em 2022")
        

    else:
        if (tabelaIBGE.Ano[0] == max(DimIBGEHistorico.Ano)
         and (mes_atual > mes_IBGEHist)):
            # UPDATE                   
            conn.execute("""
                    UPDATE hist SET 
                        hist.IDEnte = tab.IDEnte,
                        hist.Ano = tab.Ano,
                        hist.PopulacaoEstimada = tab.PopulacaoEstimada,
                        hist.PeriodoPopulacaoEstimada = tab.PeriodoPopulacaoEstimada,
                        hist.IDHM = tab.IDHM,
                        hist.PeriodoIDHM = tab.PeriodoIDHM,
                        hist.MortalidadeInfantil = tab.MortalidadeInfantil,
                        hist.PeriodoMortalidadeInfantil = tab.PeriodoMortalidadeInfantil,
                        hist.PIB = tab.PIB,
                        hist.PeriodoPIB = tab.PeriodoPIB,
                        hist.EsgotamentoSanitario = tab.EsgotamentoSanitario,
                        hist.PeriodoEsgotamentoSanitario = tab.PeriodoEsgotamentoSanitario,
                        hist.ProcessoCarga = tab.ProcessoCarga,
                        hist.DataCarga = tab.DataCarga
                    FROM dbo.DimIBGEHistorico as hist 
                    INNER JOIN dbo.tabelaIBGE as tab
                        ON hist.IDEnte = tab.IDEnte
                    WHERE tab.Ano = hist.Ano and tab.DataCarga > hist.DataCarga 
                    and tab.IDEnte = hist.IDEnte
                    """
                )
            print("A DimIBGEHistorico teve atualizacao nos dados referentes ao ano de {}!\n\n".format(tabelaIBGE.Ano[0]))
        else:
            print("A DimIBGEHistorico nao precisa ser atualizada!\n")
            
    print("\nA tabela DimIBGEHistorico foi atualizada com os dados da tabelaIBGE {}!".format(tabelaIBGE.Ano[0]))

