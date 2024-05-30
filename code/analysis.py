import pandas as pd
import numpy as np
from preprocessing import *

relevante_dcs=['0011','0042','0028','0049','0058','0099','0047','0501','0078','0005','0054','0010','0071','0057']
ursprung_dcs=['0011', '0028', '0078', '0054']

def data_loader(path, target_table) -> pd.DataFrame:
    #path: 'c:\\celver\\scm_optimization\\v_b\\new_preprocessed\\'
    #target: 'Bestelluebersicht_tabelle'
    df=pd.read_parquet(path+target_table+'.parquet')
    return df


def lieferzeiten(path, target, all_dcs, origin_dc):
    #bestellübersicht Tabelle
    bestell_df=data_loader('c:\\celver\\scm_optimization\\v_b\\new_preprocessed\\','Bestelluebersicht_tabelle')
    bestell_df.drop(['Löschkennzeichen'],axis=1,inplace=True)
    bestell_df=bestell_df[bestell_df['Werk'].isin(relevante_dcs)]
    #ekpo Tabelle
    ekpo_df=data_loader('c:\\celver\\scm_optimization\\v_b\\new_preprocessed\\','Ekbe_tabelle')
    ekpo_df=ekpo_df[(ekpo_df['BwA']==101) | (ekpo_df['BwA']==109)]
    ekpo_df=ekpo_df.groupby(['EinkBeleg', 'Pos', 'BwA','Buch.dat.'])['Menge'].sum()
    ekpo_df=ekpo_df.reset_index()
    #Tabellen mergen
    merged_df = pd.merge(bestell_df, ekpo_df, left_on=['Einkaufsbeleg','Position'], right_on=['EinkBeleg','Pos'])
    merged_df=merged_df[merged_df['noch zu liefern (Menge)']==0]

    #Tage hinzugefügt
    #gewichtete Summe gebildet
    #Jetzt kennt man Differenz aus Angelegt 
    merged_df["Tage"] = (merged_df["Buch.dat."] - merged_df["Belegdatum"]).dt.days

    def gewichtete_summe(gruppe):
        gewichtete_differenzen = gruppe['Tage'] * ( gruppe['Menge']/gruppe['Bestellmenge'] )
        return gewichtete_differenzen.sum()

    def last_day(gruppe:pd.DataFrame):
        gruppe = gruppe.sort_values(['Buch.dat.'], ascending=False)
        return gruppe.iloc[0]['Tage']

    gewichtete_summen = merged_df.groupby(['Einkaufsbeleg', 'Position','Material']).apply(gewichtete_summe)

    gewichtete_summen=gewichtete_summen.reset_index().rename(columns={0:'Tage_gewichtete_summe'})
    gewichtete_summen.head()

    letzter=merged_df.groupby(['Einkaufsbeleg', 'Position','Material']).apply(last_day)
    letzter=letzter.reset_index().rename(columns={0:'letzter_Tag'})
    letzter.head()

    #DataFrame zusammengefügt aus letzter Tag und gewichtete Summe -> getestet ist korrekt
    final_liefer_df=pd.concat([gewichtete_summen,letzter.iloc[:,-1]], axis=1)

    return final_liefer_df


#ABC XYZ Analyse

def abc_analysis(path, target_table,base,lower_bound, upper_bound) -> pd.DataFrame:
    #lower_bound='2022-01-01' , upper_bound='2022-12-31' 
    #Base= 'Menge', 'Erlös', 'NU'
    absatz_df=data_loader('c:\\celver\\scm_optimization\\v_b\\new_preprocessed\\','Absatz_tabelle')
    absatz_df=absatz_df[(absatz_df['Month']>=lower_bound) & (absatz_df['Month']<=upper_bound)].reset_index(drop=True)

    absatz_df_aggregiert=absatz_df.groupby(['Artikel'])[absatz_df.columns[-3:]].sum()
    absatz_df_aggregiert=absatz_df_aggregiert.sort_values(base,ascending=False)
    absatz_df_aggregiert=absatz_df_aggregiert.reset_index()
    summeErlöse=absatz_df_aggregiert[base].sum()
    absatz_df_aggregiert['cum_sum']=absatz_df_aggregiert[base].apply(lambda x:x/summeErlöse).cumsum()
    absatz_df_aggregiert['abc_analyse']=absatz_df_aggregiert['cum_sum'].apply(lambda x : 'A' if x < 0.8 else 'B' if x < 0.95 else 'C')
    return absatz_df_aggregiert

def xyz_analysis(path, target_table,base, lower_bound, upper_bound):
    #lower_bound='2022-01-01' , upper_bound='2022-12-31' 
    #Base= 'Menge', 'Erlös', 'NU'
    absatz_df= data_loader('c:\\celver\\scm_optimization\\v_b\\new_preprocessed\\','Absatz_tabelle')
    absatz_df=absatz_df[(absatz_df['Month']>=lower_bound) & (absatz_df['Month']<=upper_bound)].reset_index(drop=True)
    absatz_df=absatz_df.groupby(['Artikel','Month'])[base].sum()
    absatz_df=absatz_df.reset_index()

    material_pivot=pd.pivot_table(absatz_df,values=base,index='Artikel',columns='Month')
    # Mittelwert
    material_pivot["Mittelwert"] = material_pivot.mean(axis=1)

    # Standardabweichung berechnen
    material_pivot["Standardabweichung"] = material_pivot.std(axis=1)

    material_pivot['XYZ_value']=material_pivot["Standardabweichung"]/material_pivot["Mittelwert"]
    material_pivot['XYZ_Analyse']=material_pivot['XYZ_value'].apply(lambda x : 'X' if x < 1.2 else 'Y' if x < 2 else 'Z')

    return material_pivot


# df=abc_analysis('s','s','Menge','2022-01-01','2022-12-31')
# print(df.head(15))


# df=xyz_analysis('s','t','Menge','2022-01-01','2022-12-31')
# print(df.head(15))

#relevante Lieferwege ermitteln
def liefer_matrix(path, target):
    lieferwege_df=data_loader('c:\\celver\\scm_optimization\\v_b\\new_preprocessed\\','Lieferwege_tabelle')
    lieferwege_df.drop(['Unnamed: 5'],axis=1,inplace=True)
    lieferwege_df=lieferwege_df[(lieferwege_df['DC'].isin(relevante_dcs)) & (lieferwege_df['Source DC '].isin(ursprung_dcs))]
    lieferwege_df=lieferwege_df.reset_index(drop=True)
    lieferwege_df = lieferwege_df[lieferwege_df['Transshipment DC 1'].notna()]
    nodes = []
    for col in lieferwege_df.columns:
        nodes.extend(lieferwege_df[col].unique().tolist())  # Convert to list and extend

    def remove_values_from_list(the_list, val):
        return [value for value in the_list if value != val]

    nodes=remove_values_from_list(nodes,None)
    nodes=dict.fromkeys(nodes) 
    adjacency_matrix=pd.DataFrame(0,nodes,nodes,dtype=int)

    def connection(series1:pd.Series, series2:pd.Series,df):
        series1=series1.tolist()
        series2=series2.tolist()
        for i in range(0,len(series1)):
            if (series1[i]!=None) and (series2[i]!=None):
                dataFrame_index_zeile=list(df.index).index(series1[i])
                dataFrame_index_spalte=list(df.index).index(series2[i])
                df.iloc[dataFrame_index_zeile,dataFrame_index_spalte]=1

        return df

    print(lieferwege_df.head())
    adjacency_matrix=connection(lieferwege_df['Source DC '],lieferwege_df['DC'],adjacency_matrix)
    adjacency_matrix=connection(lieferwege_df['Transshipment DC 1'],lieferwege_df['Transshipment DC 2'],adjacency_matrix)
    adjacency_matrix=connection(lieferwege_df['Transshipment DC 2'],lieferwege_df['Transshipment DC 3'],adjacency_matrix)
    return adjacency_matrix


df=lieferzeiten(5,5,5,5)
save_as_parquet(df,'Lieferzeiten_berechnet_tabelle')


