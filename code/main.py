from preprocessing import *
def main():
    # #Absatz Tabelle
    # df = absatz_neu_df('xyz')
    # save_as_parquet(df,'Absatz_tabelle')

    # #Backlog Tabellen
    # df=backlog_blocked_df('xyz','wo blocked')
    # save_as_parquet(df,'Backlog_wo_blocked_tabelle')

    # df=backlog_blocked_df('xyz','incl. blocked')
    # save_as_parquet(df,'Backlog_incl._tabelle')

    # #Bestellübersicht Tabelle
    # df=bestelluebersicht_df('xyz','BestÜ neu')
    # save_as_parquet(df,'Bestelluebersicht_tabelle')

    # #Ekko Tabelle
    # df=ekko_df('xyz','EKKO neu')
    # save_as_parquet(df,'Ekko_tabelle')

    # #Ekpo Tabelle
    # df=ekpo_df('xyz','EKPO neu')
    # save_as_parquet(df,'Ekpo_tabelle')

    # #Ekbe Tabelle
    # df= ekbe_df('xyz','EKBE neu')
    # save_as_parquet(df,'Ekbe_tabelle')

    # #AFKO Tabelle
    # df= afko_df('xyz','AFKO neu')
    # save_as_parquet(df,'Afko_tabelle')

    # #AUFK Tabelle
    # df= aufk_df('xyz','AUFK neu')
    # save_as_parquet(df,'Aufk_tabelle')

    # #MVER Tabelle
    # df=mver_df('xyz','xyz')
    # save_as_parquet(df,'Mver_tabelle')

    # #IO Tabelle
    # df= io_df('xyz','IO')
    # save_as_parquet(df,'IO_tabelle')

    # #Gross Demand
    # df=gross_demand_df('xyz','Gross Demand')
    # save_as_parquet(df,'Gross_demand_tabelle')

    # #Bestandstabelle
    # df=bestand_df('xyz')
    # save_as_parquet(df,'Bestand_tabelle')

    # #Lagerkosten je DC Tabelle
    # df= lagerkosten_je_standort()
    # save_as_parquet(df,'Lagerkosten_pro_dc_tabelle')

    # #Menge pro Palette
    # df=menge_pro_palette()
    # save_as_parquet(df,'Menge_pro_pallete_tabelle')

    # #Lieferwege
    # df=lieferwege()
    # save_as_parquet(df,'Lieferwege_tabelle')

    #Lieferzeiten
    df=lieferzeiten()
    save_as_parquet(df,'Lieferzeiten_tabelle')

    # #Standardkosten
    # df=standardkosten()
    # save_as_parquet(df,'Standardkosten_tabelle')

    # #Menge pro DC
    # df=menge_pro_dc()
    # save_as_parquet(df,'Menge_pro_dc_tabelle')


    # #externe Lieferzeit
    # df=extern_lieferzeit()
    # save_as_parquet(df,'externe_lieferzeit')

    # #Dim Werk Tabelle
    # df=dim_werk()
    # save_as_parquet(df,'Dim_werk_tabelle')

    # #Service Level Tabelle
    # df=service_level()
    # save_as_parquet(df,'Service_level_tabelle')



if __name__ == "__main__":
    main()