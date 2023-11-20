import pickle
import os
import pandas as pd
from sklearn.preprocessing import StandardScaler

from feat_build import sysinfo_process
from feat_build.utils import global_data

def process_raw(df, piv_val, cols=None, agg='sum'):
    """
    processes raw by pivoting the data and given columns

    :param df: raw data as pandas DataFrame
    :param piv_val: name of pivot value column
    :param cols: list of columns to pivot on
    :param agg: aggregation function as a string ('sum', 'mean', etc.) or list of strings
                if list, must be same length as cols

    :return: processed data as pandas DataFrame
    """
    if cols and not isinstance(cols, list):
        assert isinstance(cols, list), 'cols must be a list if passed in'

    if not cols:
        cols = df.columns.tolist()

    if isinstance(agg, list):
        assert len(agg) == len(cols), 'agg and cols must be same length'
        cols = list(zip(cols, agg))
    else:
        cols = list(zip(cols, [agg] * len(cols)))

    # store pivoted dataframes to concatenate later
    piv_df = []

    for col, agg_f in cols:
        piv = pd.pivot_table(
            df, 
            values=piv_val, 
            index=['guid', 'dt'], 
            columns=[col], 
            aggfunc=agg_f, 
            fill_value=0)
        
        # rename columns
        piv.columns = [f'{col}_{i}' for i in piv.columns]

        # append pivoted dataframe to list
        piv_df.append(piv)

    # concatenate pivoted dataframes
    proc_df = pd.concat(piv_df, axis=1).reset_index()
    
    # fill missing values with 0
    proc_df.fillna(0, inplace=True)
    
    return proc_df

def proc_temp(df):
    df['prod'] = (df['nrs'] * df['avg_val']) / df.groupby(['guid', 'dt'])['nrs'].transform('sum')

    return df.groupby(['guid', 'dt'])[['prod']].sum().reset_index().rename(columns={'prod': 'temp_avg'})

def main(data_folder, proc_sysinfo=False):
    """
    main function to featureize non-sysinfo data
    
    :param data_folder: path to data folder for the investigation (not the global data folder)
    :param proc_sysinfo: boolean to process sysinfo data, 
                         should be True only if sysinfo data is new
    """
    # load software category data (from ChatGPT)
    with open(global_data / 'software_data.pkl', 'rb') as file:
        sw_cat = pickle.load(file)

    raw_folder = data_folder / 'raw'

    # load raw sample data
    sw_raw = pd.read_parquet(raw_folder / 'sw_usage.parquet')  # software usage
    web_raw = pd.read_parquet(raw_folder / 'web_usage.parquet')  # web usage
    temp_raw = pd.read_parquet(raw_folder / 'temp.parquet')  # temperature
    cpu_raw = pd.read_parquet(raw_folder / 'cpu_util.parquet')  # temperature
    power_raw = pd.read_parquet(raw_folder / 'power.parquet')  # power (predictor variable)

    # process software usage data for pivoting
    sw_raw['sw_category'] = sw_raw['frgnd_proc_name'].map(sw_cat)  # map software names to categories
    sw_raw['sw_category'] = sw_raw['sw_category'].fillna('Other')  # fill missing values with 'Other'
    
    sw_proc = process_raw(sw_raw, 'frgnd_proc_duration_ms', ['sw_category', 'sw_event_name'])
    web_proc = process_raw(web_raw, 'duration_ms', ['web_parent_category', 'web_sub_category'])
    temp_proc = proc_temp(temp_raw)

    # process CPU usage data
    cpu_raw.rename(columns={'norm_usage': 'cpu_norm_usage'}, inplace=True)

    # rename columns in power data
    power_raw.rename(columns={'mean': 'power_mean', 'nrs_sum': 'power_nrs_sum'}, inplace=True)
    power_raw.drop(columns='power_nrs_sum', inplace=True)

    # sysinfo one-hot encoding if not already done
    if 'sysinfo_ohe.parquet' not in os.listdir(global_data) or proc_sysinfo:
        sysinfo_process.main()
        
    sysinfo = pd.read_parquet(global_data / 'sysinfo_ohe.parquet')

    # merge dataframes (except power -- will merge last)
    merged_df = pd.merge(sw_proc, temp_proc, on=['guid', 'dt'], how='inner')
    merged_df = pd.merge(merged_df, web_proc, on=['guid', 'dt'], how='left')
    merged_df = pd.merge(merged_df, cpu_raw , on=['guid', 'dt'], how='inner')

    # standardize numerical columns
    scaler = StandardScaler()
    numeric_cols = merged_df.select_dtypes(include=['int', 'float']).columns.to_list()
    merged_df[numeric_cols] = scaler.fit_transform(merged_df[numeric_cols])

    # don't standardize power columns
    merged_df = pd.merge(merged_df, power_raw, on=['guid', 'dt'], how='inner')

    # merge sysinfo data
    final_df = pd.merge(merged_df, sysinfo, on='guid', how='inner')
    final_df.fillna(0, inplace=True)

    # convert dt column to datetime
    final_df['dt'] = pd.to_datetime(final_df['dt'])

    # add time features
    final_df['day_of_week'] = final_df['dt'].dt.dayofweek
    final_df['month_of_year'] = final_df['dt'].dt.month

    # drop identifier columns (just features and target left)
    final_df.drop(columns=['dt', 'guid'], inplace=True)

    # save final featureized data (including target)
    final_df.to_parquet(data_folder / 'out' / 'feat.parquet')