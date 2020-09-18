import pandas as pd
import csv


#load covid data
covid_df = pd.read_csv('WHO-COVID-19-global-data.csv')


#convert covid date to datetime64
c_dt = pd.to_datetime(covid_df['Date_reported'], format='%Y-%m-%d')
c_result = pd.concat([c_dt,covid_df.drop('Date_reported', axis=1)], axis=1)

###

#load bitcoin data
bitusd_dataset = pd.read_csv('bitcoin2.csv')
t = pd.to_datetime(bitusd_dataset.date, format='%Y-%m-%d')
b = bitusd_dataset.set_index(pd.to_datetime(bitusd_dataset.date, format='%Y-%m-%d'), 'date')
b_result = pd.concat([t, bitusd_dataset.drop('date', axis=1)], axis=1)


## Merge and add new metrics to data sets:
dict_data = []
for i in covid_df.Date_reported.sort_values().unique():
    if i == "2020-09-06": #end of bitcoin data
        break
    ### Select group by day for Covid Data:
    cr = c_result[c_result.Date_reported.between(i, i)]
    ### Select group by day for Bitcoin Data:
    br = b_result[b_result.date.between(i, i)]
    #print(i)
    dict_data.append({
        'date': i,

        'sum_new_cases': cr.groupby(['Date_reported'])['New_cases'].sum()[0],
        'kurt_new_cases': cr.groupby(['Date_reported'])['New_cases'].apply(pd.DataFrame.kurt)[0],
        'skew_new_cases': cr.groupby(['Date_reported'])['New_cases'].apply(pd.DataFrame.skew)[0],
        'mean_new_cases': cr.groupby(['Date_reported'])['New_cases'].apply(pd.DataFrame.mean)[0],

        'sum_cumulative_cases': cr.groupby(['Date_reported'])['Cumulative_cases'].sum()[0],
        'kurt_cumulative_cases': cr.groupby(['Date_reported'])['Cumulative_cases'].apply(
            pd.DataFrame.kurt)[0],
        'skew_cumulative_cases': cr.groupby(['Date_reported'])['Cumulative_cases'].apply(
            pd.DataFrame.skew)[0],
        'mean_cumulative_cases': cr.groupby(['Date_reported'])['Cumulative_cases'].apply(
            pd.DataFrame.mean)[0],

        'sum_new_deaths': cr.groupby(['Date_reported'])['New_deaths'].sum()[0],
        'kurt_new_deaths': cr.groupby(['Date_reported'])['New_deaths'].apply(pd.DataFrame.kurt)[0],
        'skew_new_deaths': cr.groupby(['Date_reported'])['New_deaths'].apply(pd.DataFrame.skew)[0],
        'mean_new_deaths': cr.groupby(['Date_reported'])['New_deaths'].apply(pd.DataFrame.mean)[0],

        'sum_cumulative_deaths': cr.groupby(['Date_reported'])['Cumulative_deaths'].sum()[0],
        'kurt_cumulative_deaths': cr.groupby(['Date_reported'])['Cumulative_deaths'].apply(pd.DataFrame.kurt)[0],
        'skew_cumulative_deaths': cr.groupby(['Date_reported'])['Cumulative_deaths'].apply(
            pd.DataFrame.skew)[0],
        'mean_cumulative_deaths': cr.groupby(['Date_reported'])['Cumulative_deaths'].apply(
            pd.DataFrame.mean)[0],

        'sum_open': br.groupby(['date'])['open'].sum()[0],
        'kurt_open': br.groupby(['date'])['open'].apply(pd.DataFrame.kurt)[0],
        'skew_open': br.groupby(['date'])['open'].apply(pd.DataFrame.skew)[0],
        'mean_open': br.groupby(['date'])['open'].apply(pd.DataFrame.mean)[0],

        'sum_close': br.groupby(['date'])['close'].sum()[0],
        'kurt_close': br.groupby(['date'])['close'].apply(pd.DataFrame.kurt)[0],
        'skew_close': br.groupby(['date'])['close'].apply(pd.DataFrame.skew)[0],
        'mean_close': br.groupby(['date'])['close'].apply(pd.DataFrame.mean)[0],

        'sum_high': br.groupby(['date'])['high'].sum()[0],
        'kurt_high': br.groupby(['date'])['high'].apply(pd.DataFrame.kurt)[0],
        'skew_high': br.groupby(['date'])['high'].apply(pd.DataFrame.skew)[0],
        'mean_high': br.groupby(['date'])['high'].apply(pd.DataFrame.mean)[0],

        'sum_low': br.groupby(['date'])['low'].sum()[0],
        'kurt_low': br.groupby(['date'])['low'].apply(pd.DataFrame.kurt)[0],
        'skew_low': br.groupby(['date'])['low'].apply(pd.DataFrame.skew)[0],
        'mean_low': br.groupby(['date'])['low'].apply(pd.DataFrame.mean)[0],

        'sum_volume': br.groupby(['date'])['volume'].sum()[0],
        'kurt_volume': br.groupby(['date'])['volume'].apply(pd.DataFrame.kurt)[0],
        'skew_volume': br.groupby(['date'])['volume'].apply(pd.DataFrame.skew)[0],
        'mean_volume': br.groupby(['date'])['volume'].apply(pd.DataFrame.mean)[0]
    })

###############

# save to file
csv_columns = [
                'date',

                'sum_new_cases',
                'kurt_new_cases',
                'skew_new_cases',
                'mean_new_cases',

                'sum_cumulative_cases',
                'kurt_cumulative_cases',
                'skew_cumulative_cases',
                'mean_cumulative_cases',

                'sum_new_deaths',
                'kurt_new_deaths',
                'skew_new_deaths',
                'mean_new_deaths',

                'sum_cumulative_deaths',
                'kurt_cumulative_deaths',
                'skew_cumulative_deaths',
                'mean_cumulative_deaths',

                'sum_open',
                'kurt_open',
                'skew_open',
                'mean_open',

                'sum_close',
                'kurt_close',
                'skew_close',
                'mean_close',

                'sum_high',
                'kurt_high',
                'skew_high',
                'mean_high',

                'sum_low',
                'kurt_low',
                'skew_low',
                'mean_low',

                'sum_volume',
                'kurt_volume',
                'skew_volume',
                'mean_volume']

csv_file = "bitcoin-covid19.csv"
try:
    with open(csv_file, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        for data in dict_data:
            writer.writerow(data)
except IOError:
    print("I/O error")