# From: https://epftoolbox.readthedocs.io/en/latest/modules/lear_examples.html#learex0

import pandas as pd
import numpy as np
import argparse
import os

from epftoolbox.data import read_data
from epftoolbox.evaluation import MAE, sMAPE
from epftoolbox.models import LEAR

# ------------------------------ EXTERNAL PARAMETERS ------------------------------------#

parser = argparse.ArgumentParser()

parser.add_argument("--dataset", type=str, default='PJM',
                    help='Market under study. If it not one of the standard ones, the file name' +
                         'has to be provided, where the file has to be a csv file')

parser.add_argument("--years_test", type=int, default=2,
                    help='Number of years (a year is 364 days) in the test dataset. Used if ' +
                         ' begin_test_date and end_test_date are not provided.')

parser.add_argument("--calibration_window", type=int, default=4 * 364,
                    help='Number of days used in the training dataset for recalibration')
# 8, 12 weeks, 3 years, 4 years
parser.add_argument("--begin_test_date", type=str, default=None,
                    help='Optional parameter to select the test dataset. Used in combination with ' +
                         'end_test_date. If either of them is not provided, test dataset is built ' +
                         'using the years_test parameter. It should either be  a string with the ' +
                         ' following format d/m/Y H:M')

parser.add_argument("--end_test_date", type=str, default=None,
                    help='Optional parameter to select the test dataset. Used in combination with ' +
                         'begin_test_date. If either of them is not provided, test dataset is built ' +
                         'using the years_test parameter. It should either be  a string with the ' +
                         ' following format d/m/Y H:M')

#args = parser.parse_args()

dataset = 'DE'
years_test = 2
begin_test_date = '4/1/2016 00:00'
end_test_date = '31/12/2017 00:00'

path_datasets_folder = os.path.join('.', 'datasets')
path_recalibration_folder = os.path.join('.', 'experimental_files')

# Defining train and testing data
df_train, df_test = read_data(dataset=dataset, years_test=years_test, path=path_datasets_folder)


# df_train, df_test = read_data(dataset=dataset, years_test=years_test, path=path_datasets_folder,
#                               begin_test_date=begin_test_date, end_test_date=end_test_date)

# Defining unique name to save the forecast

for calibration_window in [56, 84, 1092, 1456]:
    forecast_file_name = 'fc_nl' + '_dat' + str(dataset) + '_YT' + str(years_test) + \
                         '_CW' + str(calibration_window) + '.csv'

    forecast_file_path = os.path.join(path_recalibration_folder, forecast_file_name)

    # Defining empty forecast array and the real values to be predicted in a more friendly format
    forecast = pd.DataFrame(index=df_test.index[::24], columns=['h' + str(k) for k in range(24)])
    real_values = df_test.loc[:, ['Price']].values.reshape(-1, 24)
    real_values = pd.DataFrame(real_values, index=forecast.index, columns=forecast.columns)

    forecast_dates = forecast.index

    model = LEAR(calibration_window=calibration_window)

    # For loop over the recalibration dates
    for date in forecast_dates:
        # For simulation purposes, we assume that the available data is
        # the data up to current date where the prices of current date are not known
        data_available = pd.concat([df_train, df_test.loc[:date + pd.Timedelta(hours=23), :]], axis=0)

        # We set the real prices for current date to NaN in the dataframe of available data
        data_available.loc[date:date + pd.Timedelta(hours=23), 'Price'] = np.NaN

        # Recalibrating the model with the most up-to-date available data and making a prediction
        # for the next day
        Yp = model.recalibrate_and_forecast_next_day(df=data_available, next_day_date=date,
                                                     calibration_window=calibration_window)
        # Saving the current prediction
        forecast.loc[date, :] = Yp

        # Computing metrics up-to-current-date
        mae = np.mean(MAE(forecast.loc[:date].values.squeeze(), real_values.loc[:date].values))
        smape = np.mean(sMAPE(forecast.loc[:date].values.squeeze(), real_values.loc[:date].values)) * 100

        # Pringint information
        print('{} - sMAPE: {:.2f}%  |  MAE: {:.3f}'.format(str(date)[:10], smape, mae))

        # Saving forecast
        forecast.to_csv('forecasts_lear_' + str(calibration_window) + '.csv')

    print(1)