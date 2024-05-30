import pandas as pd
import numpy as np
from datetime import datetime
import re

#Bei allen Tabellen müssen noch Timestamps erzeugt werden

# Mit der Funktion werden alle Seiten in der Excel eingelesen als DataFrame in einem Dictionary. Blättername ist der Key
def save_as_parquet(df:pd.DataFrame,file_name:str):
    df.to_parquet('c:\\celver\\scm_optimization\\v_b\\new_preprocessed\\'+file_name+'.parquet')





def readData(path_excel: str) -> list:
    xls = pd.ExcelFile(path_excel)
    list = xls.sheet_names
    df_dic = dict()
    for sheet in list:
        df = pd.read_excel(xls, sheet)
        df_dic[sheet]=df
    return df_dic

def saveData(df: pd.DataFrame, path: str) -> None:
    df.to_csv(path, index=False)    

def material_dim_df(bestell_df:pd.DataFrame) -> pd.DataFrame:

    material_dim = bestell_df.groupby('Material')['Kurztext'].unique()
    material_dim = pd.DataFrame(material_dim)
    material_dim = material_dim.reset_index()
    
    material_me = bestell_df.groupby('Material')['BestellpreisME'].unique().apply(lambda arr: arr[0] if arr.size > 0 else "").reset_index(name='BestellpreisME')
    material_dim['BestellpreisME']=material_me['BestellpreisME']
    return material_dim


def bestelluebersicht_df(path:str,sheetname:str) -> pd.DataFrame:
    excel=pd.ExcelFile('c:\\celver\\scm_optimization\\v_b\\raw_data\\Beispielartikel_Bestandsoptimierung_ab2019.xlsx')
    bestell_df=pd.read_excel(excel,sheetname)
    #bestell_df.drop(['Kurztext','BestellpreisME'],axis=1,inplace=True) -> muss erstmal nicht sein
    bestell_df[['Lieferantennummer','Lieferantenname']]=bestell_df['Name des Lieferanten'].str.split(' ',n=1,expand=True)

    bestell_df.drop(['Name des Lieferanten'],axis=1,inplace=True)
    #Reihenfolge der Columns angepasst
    first_column = bestell_df.pop('Lieferantennummer')
    second_column = bestell_df.pop('Lieferantenname')  

    bestell_df.insert(0, 'Lieferantennummer', first_column) 
    bestell_df.insert(1, 'Lieferantenname', second_column) 

    #alle Leerzeichen am Anfang werden gelöscht
    bestell_df['Lieferantenname']=bestell_df['Lieferantenname'].apply(str.lstrip)
    bestell_df['Belegdatum']=bestell_df['Belegdatum'].apply(pd.to_datetime)
    bestell_df['Werk']=bestell_df['Werk'].apply(werk_str)
    return bestell_df

def gross_demand_df(path:str,sheetname:str) -> pd.DataFrame:
    excel=pd.ExcelFile('c:\\celver\\scm_optimization\\v_b\\raw_data\\Beispielartikel_Bestandsoptimierung_ab2019.xlsx')
    df=pd.read_excel(excel,sheetname)

    def convert_to_datetime_MVER(date_str):
        de_to_en_month_abbr = {
        'JAN': 'Jan',  # Januar
        'FEB': 'Feb',  # Februar
        'MAR': 'Mar',  # März
        'APR': 'Apr',  # April
        'MAI': 'May',  # Mai
        'JUN': 'Jun',  # Juni
        'JUL': 'Jul',  # Juli
        'AUG': 'Aug',  # August
        'SEP': 'Sep',  # September
        'OKT': 'Oct',  # Oktober
        'NOV': 'Nov',  # November
        'DEZ': 'Dec',   # Dezember
        'MRZ': 'Mar'
        }
        
        month = str.upper(date_str[:3])
        year = 2000+int(date_str[-2:])
        if month in de_to_en_month_abbr:
            month=de_to_en_month_abbr[month]
        date_str=f'{month} {year}'
        return pd.to_datetime(datetime.strptime(date_str, '%b %Y'))

    df.rename(columns={'DC (Gross Demand after set & network drilldown) all DC': 'Werk'}, inplace=True)
    df.drop(['Unnamed: 20', 'TOTAL'], axis=1, inplace=True)
    df['Material'] = df[df.drop(['Werk'], axis=1).isnull().all(axis=1)]['Werk']
    df['Material'] = df['Material'].fillna(method='ffill')
    df = df[df['Werk'] != df['Material']]
    # Clear lines where 'Total' appears in the Werk column
    df = df[df['Werk'].str.contains('Total') == False]  
    df = df[df['Werk'].str.contains('TOTAL') == False]
    df.fillna(0, inplace=True)
    df = pd.melt(df, id_vars=['Material', 'Werk'], var_name='Month', value_name='Gross Demand')
    df['Werk'] = df['Werk'].astype(int)
    df['Material'] = df['Material'].str[1:]
    df['Werk']=df['Werk'].apply(werk_str)
    df['Month']=df['Month'].apply(lambda x:x.replace('.',' '))
    df['Month']=df['Month'].apply(convert_to_datetime_MVER)
    return df

def ekpo_df(path:str,sheetname:str) -> pd.DataFrame:
    excel=pd.ExcelFile('c:\\celver\\scm_optimization\\v_b\\raw_data\\Beispielartikel_Bestandsoptimierung_ab2019.xlsx')
    df=pd.read_excel(excel,sheetname)
    df['Anlegedatum']=df['Anlegedatum'].apply(pd.to_datetime)
    df['Werk']=df['Werk'].apply(werk_str)
    return df

def ekko_df(path:str,sheetname:str) -> pd.DataFrame:
    excel=pd.ExcelFile('c:\\celver\\scm_optimization\\v_b\\raw_data\\Beispielartikel_Bestandsoptimierung_ab2019.xlsx')
    data_types = {"LWk": str, "Lieferant": str}
    df=pd.read_excel(excel,sheetname, dtype=data_types)
    df['Beleg_Anlege_Dat'] = df['Angel.am']
    df.drop(['Angel.am', 'BelegDat'], axis=1, inplace=True)
    return df

def ekbe_df(path:str,sheetname:str) -> pd.DataFrame:
    excel=pd.ExcelFile('c:\\celver\\scm_optimization\\v_b\\raw_data\\Beispielartikel_Bestandsoptimierung_ab2019.xlsx')
    df=pd.read_excel(excel,sheetname)    
    # df = df[df['BTy'] == 'E']
    # df = df[(df['BwA'] == 101.) | (df['BwA'] == 109.)]  
    df['Buch.dat.']=df['Buch.dat.'].apply(pd.to_datetime)
    return df

def afko_df(path:str,sheetname:str) -> pd.DataFrame:
    excel=pd.ExcelFile('c:\\celver\\scm_optimization\\v_b\\raw_data\\Beispielartikel_Bestandsoptimierung_ab2019.xlsx')
    df=pd.read_excel(excel,sheetname)   
    df[['Eckendtermin','Eckstarttermin','Iststarttermin','Istendtermin']]=df[['Eckendtermin','Eckstarttermin','Iststarttermin','Istendtermin']].apply(pd.to_datetime)
    return df

def aufk_df(path:str,sheetname:str) -> pd.DataFrame:
    excel=pd.ExcelFile('c:\\celver\\scm_optimization\\v_b\\raw_data\\Beispielartikel_Bestandsoptimierung_ab2019.xlsx')
    df=pd.read_excel(excel,sheetname)   
    df['Werk']=df['Werk'].apply(werk_str)
    df['Erf.datum']=df['Erf.datum'].apply(pd.to_datetime)
    return df


def mver_df(path:str,sheetname:str) -> pd.DataFrame:
    excel=pd.ExcelFile('c:\\celver\\scm_optimization\\v_b\\raw_data\\Beispielartikel_Bestandsoptimierung_ab2019.xlsx')
    df_1 = pd.read_excel(excel, 'MVER', nrows=74)
    df_2 = pd.read_excel(excel, 'MVER', skiprows=76, nrows=160-76)
    
    def change_month_to_monthYear(df: pd.DataFrame) -> pd.DataFrame:
        """
        Change the name of the columns to the format MonthYear
        """
        val = str(df.columns[0])[2:]
        for title in df.columns[1:]:
            df.rename(columns={title: title + '.' + val}, inplace=True)
        return df

    def change_first_three_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Change the name of the first three columns to the values of the first row
        """
        for i in range(3):
            df.rename(columns={df.columns[i]: df.iloc[0, i]}, inplace=True)
        return df
    
    def convert_to_datetime_MVER(date_str):
        de_to_en_month_abbr = {
        'JAN': 'Jan',  # Januar
        'FEB': 'Feb',  # Februar
        'MAR': 'Mar',  # März
        'APR': 'Apr',  # April
        'MAI': 'May',  # Mai
        'JUN': 'Jun',  # Juni
        'JUL': 'Jul',  # Juli
        'AUG': 'Aug',  # August
        'SEP': 'Sep',  # September
        'OKT': 'Oct',  # Oktober
        'NOV': 'Nov',  # November
        'DEZ': 'Dec',   # Dezember
        'MRZ': 'Mar'
        }
        
        month = str.upper(date_str[:3])
        year = 2000+int(date_str[-2:])
        if month in de_to_en_month_abbr:
            month=de_to_en_month_abbr[month]
        date_str=f'{month} {year}'
        return pd.to_datetime(datetime.strptime(date_str, '%b %Y'))

    df_2 = change_month_to_monthYear(df_2)
    df_1 = change_month_to_monthYear(df_1)

    df_2 = change_first_three_columns(df_2)
    df_1 = change_first_three_columns(df_1)
    df_2 = df_2.iloc[1:]
    df_1 = df_1.iloc[1:]

    df_1.drop(['Jahr'], axis=1, inplace=True)
    df_2.drop(['Jahr'], axis=1, inplace=True)
    df_1 = pd.melt(df_1, id_vars=['Material', 'Werk'], var_name='Month', value_name='Absatz')
    df_2 = pd.melt(df_2, id_vars=['Material', 'Werk'], var_name='Month', value_name='Absatz')

    df_final= pd.concat([df_1,df_2], axis=0, ignore_index=True)

    # df = df_1.merge(df_2, on=['Material', 'Werk'], how='outer')
    # df = pd.melt(df, id_vars=['Material', 'Werk'], var_name='Month', value_name='Absatz')
    df_final['Month']=df_final['Month'].apply(lambda x: x.replace('.',' '))
    df_final['Month']=df_final['Month'].apply(convert_to_datetime_MVER)
    return df_final

def dim_werk_df(werk_df: pd.DataFrame) -> pd.DataFrame:
    return werk_df[['Werk', 'Geschäftsj./Periode']].drop_duplicates().reset_index()
    
def convert_to_datetime(date_str):
    de_to_en_month_abbr = {
    'JAN': 'Jan',  # Januar
    'FEB': 'Feb',  # Februar
    'MAR': 'Mar',  # März
    'APR': 'Apr',  # April
    'MAI': 'May',  # Mai
    'JUN': 'Jun',  # Juni
    'JUL': 'Jul',  # Juli
    'AUG': 'Aug',  # August
    'SEP': 'Sep',  # September
    'OKT': 'Oct',  # Oktober
    'NOV': 'Nov',  # November
    'DEZ': 'Dec'   # Dezember
    
    }
    
    month = str.upper(date_str[:3])
    year = int(date_str[-4:])
    if month in de_to_en_month_abbr:
        month=de_to_en_month_abbr[month]
    date_str=f'{month} {year}'
    return pd.to_datetime(datetime.strptime(date_str, '%b %Y'))
   


def absatz_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.iloc[1:]
    df.drop(['Gesamtergebnis'], axis=1, inplace=True)
    df.rename({'Unnamed: 1':'Kurztext','Geschäftsj./Periode':'Gesellschaft'},axis=1, inplace=True)
    df=df.melt(id_vars=df.columns[:4],var_name='Month', value_name='Absatz')
    df['Werk']=df['Werk'].apply(int) 
    df['Absatz']=data['Absatz'].fillna(0)
    df['Month'] = df['Month'].apply(convert_to_datetime)
    return df

# neue Absatztabelle mit Daten ab 2019
def absatz_neu_df(path:str) -> pd.DataFrame: 
    excel=pd.ExcelFile('c:\\celver\\scm_optimization\\v_b\\raw_data\\Absatz 012019 - 022024.xlsx')
    df=pd.read_excel(excel,0)
    df=df.iloc[12:]
    df.reset_index(drop=True, inplace=True)
    df.columns=df.iloc[0]
    df.drop(0,inplace=True)
    cols=df.columns.tolist()
    cols[0]=df.iloc[0,0]
    cols[1]='Kurztext'
    cols[2]=df.iloc[0,2]
    cols[3]='Gesellschaft'
    df.columns=cols
    df=df.iloc[2:]
    menge_df=df.iloc[:,:66]
    erlöse_df=df.iloc[:,66:128]
    nu_df=df.iloc[:,128:]

    menge_df=menge_df.melt(id_vars=menge_df.columns[0:4], var_name='Month', value_name='Menge')
    erlöse_df=erlöse_df.melt(var_name='Month', value_name='Erlös')
    erlöse_df.drop(['Month'],axis=1, inplace= True)
    nu_df=nu_df.melt(var_name='Month', value_name='NU')
    nu_df.drop(['Month'],axis=1, inplace= True)

    final_df=pd.concat([menge_df,erlöse_df,nu_df],axis= 1)
    final_df[['Menge','Erlös','NU']]=final_df[['Menge','Erlös','NU']].fillna(0)
    final_df['Month']=final_df['Month'].apply(convert_to_datetime)
    return final_df


#beim einlesen muss der Parameter header = 1 gesetzt werden

def io_df(path:str,sheetname:str) -> pd.DataFrame:
    excel=pd.ExcelFile('c:\\celver\\scm_optimization\\v_b\\raw_data\\Beispielartikel_Bestandsoptimierung_ab2019.xlsx')
    daten=pd.read_excel(excel,sheetname,header=1)     
    daten.rename(columns={'Year': 'Werk'}, inplace=True)
    daten.drop(columns=['Unnamed: 25'], inplace=True)

    def convert_to_datetime_MVER(date_str):
        de_to_en_month_abbr = {
        'JAN': 'Jan',  # Januar
        'FEB': 'Feb',  # Februar
        'MAR': 'Mar',  # März
        'APR': 'Apr',  # April
        'MAI': 'May',  # Mai
        'JUN': 'Jun',  # Juni
        'JUL': 'Jul',  # Juli
        'AUG': 'Aug',  # August
        'SEP': 'Sep',  # September
        'OKT': 'Oct',  # Oktober
        'NOV': 'Nov',  # November
        'DEZ': 'Dec',   # Dezember
        'MRZ': 'Mar'
        }
        
        month = str.upper(date_str[:3])
        year = 2000+int(date_str[-2:])
        if month in de_to_en_month_abbr:
            month=de_to_en_month_abbr[month]
        date_str=f'{month} {year}'
        return pd.to_datetime(datetime.strptime(date_str, '%b %Y'))


    # Entfernen der letzten zwei Zeilen
    daten = daten.iloc[:-2]

    # Entfernen von führenden Leerzeichen in der 'Werk'-Spalte und Filtern von Zeilen, die mit "Total" beginnen
    daten['Werk'] = daten['Werk'].str.strip()
    daten = daten[~daten['Werk'].str.startswith('Total')]

    # Erstellen einer neuen Spalte, die Werte aus 'Werk' übernimmt, wenn sie mit '-' beginnen
    # und den vorherigen Wert beibehält, falls sie nicht mit '-' beginnen, 
    # mit Hilfe der forward fill Methode
    daten['neue Spalte'] = daten['Werk'].apply(lambda x: x if x.startswith('-') else None)
    daten['neue Spalte'].ffill(inplace=True)

    # Entfernen des ersten Zeichens (vermutlich '-') und führender Leerzeichen in 'neue Spalte'
    daten['neue Spalte'] = daten['neue Spalte'].str[1:].str.strip()

    # Aufteilen der 'neue Spalte' in 'material' und 'kurztext' basierend auf dem ersten Leerzeichen
    daten[['material', 'kurztext']] = daten['neue Spalte'].str.split(' ', n=1, expand=True)

    # Verschieben der neuen Spalten an den Anfang des DataFrames
    cols = daten.columns.tolist()
    cols = cols[-3:] + cols[:-3]
    daten = daten[cols]


    # Umschmelzen des DataFrames, um ein langes Format zu erhalten
    daten = daten[~daten['Werk'].str.startswith('-')]
    daten = daten.melt(id_vars=daten.columns[:4], var_name='Month', value_name='Absatz')

    # Extrahieren der letzten 6 Zeichen der 'Month'-Spalte für das Monatsformat
    daten['Month'] = daten['Month'].str[-6:]
    daten[['Werksnummer','Werksname']]=daten['Werk'].str.split(' ', n=1, expand=True)
    daten.drop(['Werk'], axis=1, inplace=True)


    # Zurücksetzen des Indexes, um nach dem Löschen von Zeilen einen sauberen Index zu haben
    daten.reset_index(drop=True, inplace=True)
    daten.drop(['neue Spalte'], axis=1,inplace=True)
    cols = daten.columns.tolist()
    cols = cols[-2:] + cols[:-2]
    daten = daten[cols]

    #Nan mit 0 auffüllen
    daten['Absatz']=daten['Absatz'].fillna(0)
    daten['Month']=daten['Month'].apply(convert_to_datetime_MVER)

    return daten

#Funktion, um die Bestandsdaten zu proprocessen
def bestand_df(path:str) -> pd.DataFrame:
    excel=pd.ExcelFile('c:\\celver\\scm_optimization\\v_b\\raw_data\\Bestand 012019 - 022024.xlsx')
    bestand_df=pd.read_excel(excel,0,header=14)
    bedingung_st=bestand_df.iloc[0]=='ST'
    bedingung_eur=bestand_df.iloc[0]=='EUR'
    # Maske für nur die Mengen und Bruttowert
    mengen_ST=bestand_df.loc[:,bedingung_st]
    mengen_ST=mengen_ST.iloc[2:]

    bruttowert_eur=bestand_df.loc[:,bedingung_eur]
    bruttowert_eur=bruttowert_eur.iloc[2:]
    cols=bestand_df.columns.to_list()
    cols[0]='Artikel'
    cols[1]='Kurztext'
    cols[2]='Werk'
    cols[3]='Name des Werks'
    bestand_df.columns=cols
    bestand_df=bestand_df.iloc[2:,:4]
    bestand_menge=pd.concat([bestand_df,mengen_ST],axis= 1)
    #Dataframes in richtige Form bringen
    bestand_menge=bestand_menge.melt(id_vars=bestand_menge.columns[:4],var_name='Month', value_name='Menge in ST')
    bruttowert_eur=bruttowert_eur.melt(var_name='Variable', value_name='Bruttowert in EUR').drop(['Variable'],axis=1)

    final_df= pd.concat([bestand_menge,bruttowert_eur],axis=1)
    final_df=final_df.fillna({"Menge in ST": 0, "Bruttowert in EUR": 0})
    final_df['Month']=final_df['Month'].apply(lambda x: pd.to_datetime(x, format='%m.%Y'))
    return final_df

def lagerkosten_je_standort():
    excel=pd.ExcelFile('c:\\celver\\scm_optimization\\v_b\\raw_data\\Lagerkosten je DC.xlsx')
    lagerkosten_df=pd.read_excel(excel,'Lagerkosten je Standort', header=1)
    lagerkosten_df=lagerkosten_df.iloc[0:8,1:]
    lagerkosten_df=lagerkosten_df.rename(columns={"Unnamed: 1": "Standort"})
    lagerkosten_df['DC']=lagerkosten_df['DC'].apply(int)
    lagerkosten_df['DC']=lagerkosten_df['DC'].apply(werk_str)
    return lagerkosten_df

def menge_pro_palette():
    excel=pd.ExcelFile('c:\\celver\\scm_optimization\\v_b\\raw_data\\Lagerkosten je DC.xlsx')
    pallete_df=pd.read_excel(excel,'Menge pro Palette')
    pallete_df=pallete_df.iloc[:-1]
    pallete_df['Article']=pallete_df['Article'].apply(str)
    return pallete_df

def lieferwege():
    excel=pd.ExcelFile('c:\\celver\\scm_optimization\\v_b\\raw_data\\Lieferzeiten und Lieferwege DC-Netzwerk.xlsx')
    lieferwege_df=pd.read_excel(excel,'interne Lieferwege',dtype=str)
    return lieferwege_df

def lieferzeiten():
    excel=pd.ExcelFile('c:\\celver\\scm_optimization\\v_b\\raw_data\\Lieferzeiten und Lieferwege DC-Netzwerk.xlsx')
    lieferzeiten_df=pd.read_excel(excel,'Lieferzeiten_gefiltert',dtype=str)
    lieferzeiten_df=lieferzeiten_df.iloc[:,:3]
    cols=lieferzeiten_df.columns.to_list()
    cols[2]="Lieferzeiten in d"
    cols=[str.strip(x) for x in cols]
    lieferzeiten_df.columns=cols
    return lieferzeiten_df

def standardkosten():
    excel=pd.ExcelFile('c:\\celver\\scm_optimization\\v_b\\raw_data\\Standardkosten &-lieferzeit.xlsx')
    standardkosten_df=pd.read_excel(excel,0)
    standardkosten_df['Artikel']=standardkosten_df['Artikel'].apply(str)
    return standardkosten_df


def menge_pro_dc():
    excel=pd.ExcelFile('c:\\celver\\scm_optimization\\v_b\\raw_data\\Verfügbare Menge pro DC.xlsx')
    file=pd.ExcelFile('c:\\celver\\scm_optimization\\v_b\\raw_data\\Verfügbare Menge pro DC.xlsx')


    def data_man(werk:str,number_of_sheet:int):
        menge_dc_df=pd.read_excel(excel,number_of_sheet)
        #alle Spalten führende Leerzeichen wegmachen
        cols=menge_dc_df.columns.to_list()
        cols=[str.strip(x) for x in cols]
        menge_dc_df.columns=cols
        #alle unnamed Spalten raus
        menge_dc_df = menge_dc_df.loc[:, ~menge_dc_df.columns.str.contains('^Unnamed')]
        menge_dc_df.drop(['Summe:'], axis=1,inplace= True)
        menge_dc_df=menge_dc_df.iloc[:-2]
        menge_dc_df=menge_dc_df.melt(id_vars=menge_dc_df.columns[0],var_name='Material',value_name='Menge')
        menge_dc_df[['Material','Kurztext']]=menge_dc_df['Material'].str.split(" ",n=1,expand=True)
        cols=menge_dc_df.columns.to_list()
        #Reihenfolge der Spalten angepasst
        menge_dc_df=menge_dc_df[cols[0:2]+cols[-1:]+cols[2:3]]
        menge_dc_df['Werk']=werk
        
        return menge_dc_df


    final_df=pd.DataFrame()
    for i in range(0,len(file.sheet_names)):
        data=data_man(file.sheet_names[i],i)
        final_df=pd.concat([final_df,data],axis=0)
    final_df.reset_index(drop=True,inplace=True)
    final_df['Monat'] = final_df['Monat'].apply(lambda x: pd.to_datetime(x.strftime('%Y-%m-01')))
    pattern = re.compile(r'\b\d{4}\b')
    def extract_number(s):
        match = pattern.search(s)
        return match.group() if match else None
    final_df['Werk']=final_df['Werk'].apply(extract_number)

    return final_df

def backlog_blocked_df(path:str,sheetname:str):
    excel=pd.ExcelFile('c:\\celver\\scm_optimization\\v_b\\raw_data\\Backlog 012019 - 022024.xlsx')
    backlog_blocked_df=pd.read_excel(excel,sheetname,header=1)
    backlog_blocked_df=backlog_blocked_df.iloc[1:]
    backlog_blocked_df.dropna(inplace=True)
    backlog_blocked_df.reset_index(drop=True, inplace=True)
    cols=backlog_blocked_df.columns.to_list()
    cols[0],cols[1],cols[2],cols[3]='Material und Werk','Material','Kurztext','Werk'
    backlog_blocked_df.columns=cols
    backlog_blocked_df['Werk']=backlog_blocked_df['Werk'].apply(int)
    backlog_blocked_df=backlog_blocked_df.melt(id_vars=backlog_blocked_df.columns[:4],var_name='Month', value_name='Menge')
    #Datum im richtigen Format
    backlog_blocked_df['Month']=backlog_blocked_df['Month'].apply(pd.to_datetime)
    #Ursprünglichen String wieder generiert
    backlog_blocked_df['Werk']=backlog_blocked_df['Werk'].apply(werk_str)
    return backlog_blocked_df

#Eine Funktion, um den Ursprüunglichen Werksstring zu generieren
def werk_str(x):
    x=str(x)
    length_werk=len(x)
    diff=4-length_werk
    final_werk= diff*'0'+x
    return final_werk


def extern_lieferzeit() -> pd.DataFrame:
    d = {'DC0': ['0011','0011','0011','0011','0011','0011','0011','0028','0028','0078','0054','0054'], 'AT EigenF': [30,30,30,0,0,0,0,0,0,7,15,15],'KT PLZ':[1,1,1,7,28,28,56,60,60,1,0,0],'AT WEBZ':[3,3,3,3,3,3,3,0,0,1,1,1],'Summe Kalendertage':[47.2,47.2,47.2,11.2,32.2,32.2,60.2,60,60,12.2,22.4,22.4]}

    new_data=pd.DataFrame(data=d, index=['560050R1P','61630001','5614R001P','9M78S101','92249068','92218068','92219900','TVS00035100000','TVS10535311061','UBQ170OBE2V-01','FG0603809','FP603476'])
    return new_data

def dim_werk():
    bestand_df=pd.read_parquet('c:\\celver\\scm_optimization\\v_b\\new_preprocessed\\'+'Bestand_tabelle'+'.parquet')
    a=bestand_df.groupby(['Werk'])['Name des Werks'].apply(lambda x: x.iloc[0])
    a=a.reset_index()
    return a

def service_level():
    data={'Kombination':['AX','AY','AZ','BX','BY','BZ','CX','CY','CZ'],'Service Level':[0.95,0.95,0.92,0.96,0.96,0.90,0.94,0.92,0.90]}
    df= pd.DataFrame(data)
    return df