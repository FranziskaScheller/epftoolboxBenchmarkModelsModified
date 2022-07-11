import pandas as pd
from datetime import timedelta, datetime
import numpy as np

AmpirionLoadForecast = pd.read_csv('netzlastnachfrage-01.01.2011 00_00.csv', sep=';', low_memory=True, decimal = ',')
Price = pd.read_excel('Prices_2010-2020.xlsx')

Price['UTC Timestamp'] = pd.Series(Price['UTC Timestamp']).round('60min')
Price['CET Timestamp'] = Price['UTC Timestamp'] + timedelta(hours=1)
Price = Price[Price['CET Timestamp'].dt.year >= 2011].reset_index().drop(columns = 'index')
Price = Price[['CET Timestamp', 'price day ahead actual']]
Price_old = Price
#Dates_TC_plus = ['2017-03-26 02:00:00','2017-03-26 02:00:00', '2018-03-25 02:00:00','2018-03-25 02:00:00', '2019-03-31 02:00:00', '2019-03-31 02:00:00', '2020-03-29 02:00:00', '2020-03-29 02:00:00']

Dates_TC_plus = [['2011-03-27 02:00:00', '2011-10-30 02:00:00'], ['2012-03-25 02:00:00', '2012-10-28 02:00:00'], ['2013-03-31 02:00:00', '2013-10-27 02:00:00'], ['2014-03-30 02:00:00', '2014-10-26 02:00:00'], ['2015-03-29 02:00:00', '2015-10-25 02:00:00'], ['2016-03-27 02:00:00', '2016-10-30 02:00:00'], ['2017-03-26 02:00:00', '2017-10-29 02:00:00'], ['2018-03-25 02:00:00','2018-10-28 02:00:00'], ['2019-03-31 02:00:00', '2019-10-27 02:00:00'], ['2020-03-29 02:00:00', '2020-10-25 02:00:00']]
Dates_TC_minus = ['2017-03-26 02:00:00', '2018-03-25 02:00:00', '2019-03-31 02:00:00', '2020-03-29 02:00:00']
ind = 0
for date in Dates_TC_plus:
    if ind == 0:
        date_ts = datetime.strptime(date[0], "%Y-%m-%d %H:%M:%S")
        ind_date = Price[Price['CET Timestamp'] == date_ts].index.values
        new_val = np.mean(Price['price day ahead actual'].iloc[ind_date[0] - 1:ind_date[0]+1])

        Prices_b = Price.iloc[:ind_date[0]]
        Prices_a = Price.iloc[ind_date[0]:]
        Prices_b.loc[len(Prices_b.index)] = [date_ts, new_val]

        Prices_a['CET Timestamp'] = Prices_a['CET Timestamp'] + timedelta(hours=1)

        Price = Prices_b.append(Prices_a)
        Price = Price.reset_index().drop(columns = 'index')

        date_ts = datetime.strptime(date[1], "%Y-%m-%d %H:%M:%S")
        ind_date = Price[Price['CET Timestamp'] == date_ts].index.values
        new_val = np.mean(Price['price day ahead actual'].iloc[ind_date[0]:ind_date[0]+2])

        Prices_b = Price.iloc[:ind_date[0]]
        Prices_a = Price.iloc[ind_date[0]+2:]
        Prices_a['CET Timestamp'] = Prices_a['CET Timestamp'] - timedelta(hours=1)

        Prices_b.loc[len(Prices_b.index)] = [date_ts, new_val]
        Price = Prices_b.append(Prices_a)

        Price = Price.reset_index().drop(columns='index')


AmpirionLoadForecast['Uhrzeit_Start'] = [x[:5] for x in AmpirionLoadForecast['Uhrzeit']]
AmpirionLoadForecast['CET Timestamp'] = pd.to_datetime(AmpirionLoadForecast['Datum'].astype(str) + ' ' + AmpirionLoadForecast['Uhrzeit_Start'].astype(str), format = '%d.%m.%Y %H:%M')
AmpirionLoadForecast['CET Timestamp'] = pd.Series(AmpirionLoadForecast['CET Timestamp'].apply(lambda x: x.floor('H')))

AmpirionLoadForecast_aggr = AmpirionLoadForecast[['CET Timestamp', 'Prognose']].dropna().reset_index().drop(columns = 'index')

AmpirionLoadForecast_aggr1 = AmpirionLoadForecast_aggr.groupby(by=['CET Timestamp']).mean()

solar1 = pd.read_csv('pv_einspeisung-01.01.2011 00_00.csv', sep=';', low_memory=True, decimal = ',')

solar1['Uhrzeit_Start'] = [x[:5] for x in solar1['Uhrzeit']]
solar1['CET Timestamp'] = pd.to_datetime(solar1['Datum'].astype(str) + ' ' + solar1['Uhrzeit_Start'].astype(str), format = '%d.%m.%Y %H:%M')
solar1['CET Timestamp'] = pd.Series(solar1['CET Timestamp'].apply(lambda x: x.floor('H')))

solar1_aggr = solar1[['CET Timestamp', '8:00 Uhr Prognose [MW]']].dropna().reset_index().drop(columns = 'index')
#solar1_aggr = solar1[['CET Timestamp', 'Online Hochrechnung [MW]']].dropna().reset_index().drop(columns = 'index')

solar1_aggr = solar1_aggr.groupby(by=['CET Timestamp']).mean()


wind1 = pd.read_csv('winddaten-01.01.2011 00_00.csv', sep=';', low_memory=True, decimal = ',')
wind1['Uhrzeit_Start'] = [x[:5] for x in wind1['Uhrzeit']]
wind1['CET Timestamp'] = pd.to_datetime(wind1['Datum'].astype(str) + ' ' + wind1['Uhrzeit_Start'].astype(str))
wind1['CET Timestamp'] = pd.Series(wind1['CET Timestamp'].apply(lambda x: x.floor('H')))

wind1_aggr = wind1[['CET Timestamp', '8:00 Uhr Prognose [MW]']].dropna().reset_index().drop(columns = 'index')
#wind1_aggr = wind1[['CET Timestamp', 'Online Hochrechnung [MW]']].dropna().reset_index().drop(columns = 'index')

wind1_aggr = wind1_aggr.groupby(by=['CET Timestamp']).mean()

#todo: online hochrechnung oder 8 Uhr prognose?

solar2 = pd.read_csv('solarEnergyFeedIn_ALL_2011-01-01_2020-12-31.csv', sep=';', low_memory=True, decimal = ',')

solar2['CET Timestamp'] = pd.to_datetime(solar2['Datum'].astype(str) + ' ' + solar2['Startzeit'].astype(str), format = '%Y-%m-%d %H:%M:%S')
solar2['CET Timestamp'] = pd.Series(solar1['CET Timestamp'].apply(lambda x: x.floor('H')))

solar2_aggr = solar2[['CET Timestamp', 'Prognostiziert in MW']].dropna().reset_index().drop(columns = 'index')

solar2_aggr = solar2_aggr.groupby(by=['CET Timestamp']).mean()

wind2 = pd.read_csv('windPowerFeedIn_2011-01-01_2020-12-31.csv', sep=';', low_memory=True, decimal = ',')

wind2['CET Timestamp'] = pd.to_datetime(wind2['Datum'].astype(str) + ' ' + wind2['Startzeit'].astype(str), format = '%Y-%m-%d %H:%M:%S')
wind2['CET Timestamp'] = pd.Series(wind2['CET Timestamp'].apply(lambda x: x.floor('H')))

wind2_aggr = wind2[['CET Timestamp', 'prognostiziert [MW]']].dropna().reset_index().drop(columns = 'index')

wind2_aggr = wind2_aggr.groupby(by=['CET Timestamp']).mean()

solar3_2011 = pd.read_csv('Solarenergie_Prognose_2011.csv', sep = ';')
solar3_2012 = pd.read_csv('Solarenergie_Prognose_2012.csv', sep = ';')
solar3_2013 = pd.read_csv('Solarenergie_Prognose_2013.csv', sep = ';')
solar3_2014 = pd.read_csv('Solarenergie_Prognose_2014.csv', sep = ';')
solar3_2015 = pd.read_csv('Solarenergie_Prognose_2015.csv', sep = ';')
solar3_2016 = pd.read_csv('Solarenergie_Prognose_2016.csv', sep = ';')
solar3_2017 = pd.read_csv('Solarenergie_Prognose_2017.csv', sep = ';')
solar3_2018 = pd.read_csv('Solarenergie_Prognose_2018.csv', sep = ';')
solar3_2019 = pd.read_csv('Solarenergie_Prognose_2019.csv', sep = ';')
solar3_2020 = pd.read_csv('Solarenergie_Prognose_2020.csv', sep = ';')

solar3 = pd.concat([solar3_2011, solar3_2012, solar3_2013, solar3_2014, solar3_2015, solar3_2016, solar3_2017, solar3_2018, solar3_2019, solar3_2020])

solar3['CET Timestamp'] = pd.to_datetime(solar3['Datum'].astype(str) + ' ' + solar3['Von'].astype(str), format = '%d.%m.%y %H:%M')
solar3['CET Timestamp'] = pd.Series(solar3['CET Timestamp'].apply(lambda x: x.floor('H')))

solar3_aggr = solar3[['CET Timestamp', 'MW']].dropna().reset_index().drop(columns = 'index')

solar3_aggr = solar3_aggr.groupby(by=['CET Timestamp']).mean()

wind3_2011 = pd.read_csv('Windenergie_Prognose_2011.csv', sep = ';', decimal = ',')
wind3_2012 = pd.read_csv('Windenergie_Prognose_2012.csv', sep = ';', decimal = ',')
wind3_2013 = pd.read_csv('Windenergie_Prognose_2013.csv', sep = ';', decimal = ',')
wind3_2014 = pd.read_csv('Windenergie_Prognose_2014.csv', sep = ';', decimal = ',')
wind3_2015 = pd.read_csv('Windenergie_Prognose_2015.csv', sep = ';', decimal = ',')
wind3_2016 = pd.read_csv('Windenergie_Prognose_2016.csv', sep = ';', decimal = ',')
wind3_2017 = pd.read_csv('Windenergie_Prognose_2017.csv', sep = ';', decimal = ',')
wind3_2018 = pd.read_csv('Windenergie_Prognose_2018.csv', sep = ';', decimal = ',')
wind3_2019 = pd.read_csv('Windenergie_Prognose_2019.csv', sep = ';', decimal = ',')
wind3_2020 = pd.read_csv('Windenergie_Prognose_2020.csv', sep = ';', decimal = ',')

wind3 = pd.concat([wind3_2011, wind3_2012, wind3_2013, wind3_2014, wind3_2015, wind3_2016, wind3_2017, wind3_2018, wind3_2019, wind3_2020])

wind3['CET Timestamp'] = pd.to_datetime(wind3['Datum'].astype(str) + ' ' + wind3['Von'].astype(str), format = '%d.%m.%y %H:%M')
wind3['CET Timestamp'] = pd.Series(wind3['CET Timestamp'].apply(lambda x: x.floor('H')))

wind3_aggr = wind3[['CET Timestamp', 'MW']].dropna().reset_index().drop(columns = 'index')

wind3_aggr = wind3_aggr.groupby(by=['CET Timestamp']).mean()

DE11to20 = Price[['CET Timestamp', 'price day ahead actual']].merge(AmpirionLoadForecast_aggr1, on='CET Timestamp', how='left')
DE11to20 = DE11to20.merge(solar1_aggr, on='CET Timestamp', how='left')
DE11to20 = DE11to20.merge(solar2_aggr, on='CET Timestamp', how='left')
DE11to20 = DE11to20.merge(solar3_aggr, on='CET Timestamp', how='left')
DE11to20 = DE11to20.merge(wind1_aggr, on='CET Timestamp', how='left')
DE11to20 = DE11to20.merge(wind2_aggr, on='CET Timestamp', how='left')
DE11to20 = DE11to20.merge(wind3_aggr, on='CET Timestamp', how='left')

DE11to20['Price'] = DE11to20['price day ahead actual']
DE11to20['Ampirion Load Forecast'] = DE11to20['Prognose']
DE11to20['PV+Wind Forecast'] = DE11to20['8:00 Uhr Prognose [MW]_x'] + DE11to20['8:00 Uhr Prognose [MW]_y'] + DE11to20['Prognostiziert in MW'] + DE11to20['MW_x'] + DE11to20['MW_y'] + DE11to20['prognostiziert [MW]']
#DE17to20['PV+Wind Forecast'] = DE17to20['Online Hochrechnung [MW]_x'] + DE17to20['Online Hochrechnung [MW]_y'] + DE17to20['Prognostiziert in MW'] + DE17to20['MW_x'] + DE17to20['MW_y'] + DE17to20['prognostiziert [MW]']

DE11to20 = DE11to20.drop(columns = ['price day ahead actual', 'Prognose', '8:00 Uhr Prognose [MW]_x', '8:00 Uhr Prognose [MW]_y', 'Prognostiziert in MW', 'MW_x', 'MW_y', 'prognostiziert [MW]'])

for date in Dates_TC_plus:
    ind_row = DE11to20['Ampirion Load Forecast'][DE11to20['CET Timestamp'] == date[0]].index
    DE11to20['Ampirion Load Forecast'].iloc[ind_row] = DE11to20['Ampirion Load Forecast'].iloc[ind_row-1]
    DE11to20['PV+Wind Forecast'].iloc[ind_row] = DE11to20['PV+Wind Forecast'].iloc[ind_row-1]

DE11to20 = DE11to20.fillna(method='ffill')

DE11to20.to_csv('DE11to20.csv', index = False)
print(1)