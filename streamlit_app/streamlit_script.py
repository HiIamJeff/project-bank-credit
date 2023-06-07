
import pandas as pd
from pandas.api.types import is_categorical_dtype, is_datetime64_any_dtype, is_numeric_dtype, is_object_dtype
import plotly.express as px
import json
import pickle
import re
import geopandas

import streamlit as st


with open('streamlit_app/list_zipcode_nyc.pkl', 'rb') as f:
    list_zipcode_nyc = pickle.load(f)

with open('streamlit_app/zipcode_2020_shape_simplified.geojson', 'rb') as response:
    zipcode_geojson = json.load(response)


def load_report(period='2019/04', first_load_flag=False):
    """first_load_flag is for the first df load that is only used for getting columns to build options
    """
    if first_load_flag:
        return pd.read_csv(f"data/processed/{period}/report/{re.sub('/', '', period)}_monthly_report.csv",
                     dtype={'zip5': 'str'}, nrows=1)

    df = pd.read_csv(f"data/processed/{period}/report/{re.sub('/', '', period)}_monthly_report.csv",
                     dtype={'zip5': 'str'})
    df = df[df['zip5'].isin(list_zipcode_nyc)] # additional filter for the records to be within NYC area
    return df


def plot_choropleth(df, metric='homebuyers_total'):
    fig = px.choropleth(df,
                        geojson=zipcode_geojson,
                        locations='zip5',
                        color=metric,
                        color_continuous_scale="blues",
                        locationmode='geojson-id',
                        featureidkey="properties.ZCTA5CE20",
                        scope="usa")
    fig.update_geos(fitbounds="locations", visible=False)

    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    return fig


def generate_st_filter_df(df):
    """Add a UI on top of a dataframe to let viewers filter columns
    """
    # optional filter button
    modify = st.checkbox("Add filters")
    if not modify:
        return df

    df = df.copy()

    # Try to convert datetimes into a standard format (datetime, no timezone)
    for col in df.columns:
        if is_object_dtype(df[col]):
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass

        if is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(None)

    modification_container = st.container()

    with modification_container:
        to_filter_columns = st.multiselect("Filter dataframe on", df.columns)
        for column in to_filter_columns:
            left, right = st.columns((1, 20))
            # Treat columns with < 10 unique values as categorical
            if is_categorical_dtype(df[column]) or df[column].nunique() < 10:
                user_cat_input = right.multiselect(
                    f"Values for {column}",
                    df[column].unique(),
                    default=list(df[column].unique()),
                )
                df = df[df[column].isin(user_cat_input)]
            elif is_numeric_dtype(df[column]):
                _min = float(df[column].min())
                _max = float(df[column].max())
                step = (_max - _min) / 100
                user_num_input = right.slider(
                    f"Values for {column}",
                    min_value=_min,
                    max_value=_max,
                    value=(_min, _max),
                    step=step,
                )
                df = df[df[column].between(*user_num_input)]
            elif is_datetime64_any_dtype(df[column]):
                user_date_input = right.date_input(
                    f"Values for {column}",
                    value=(
                        df[column].min(),
                        df[column].max(),
                    ),
                )
                if len(user_date_input) == 2:
                    user_date_input = tuple(map(pd.to_datetime, user_date_input))
                    start_date, end_date = user_date_input
                    df = df.loc[df[column].between(start_date, end_date)]
            else:
                user_text_input = right.text_input(
                    f"Substring or regex in {column}",
                )
                if user_text_input:
                    df = df[df[column].astype(str).str.contains(user_text_input, flags=re.IGNORECASE)]
    return df


def simplify_shp_file_to_geojson(input_path_shp, level=200, output_path='new_shape.geojson'):
    """Process shape files (simplify it and convert it into geojson)
    p.s. the higher the level is, the more generalization it would get
    """
    geopandas_df_shp = geopandas.read_file(input_path_shp)
    df_new = geopandas_df_shp.copy(deep=True)
    geocol = df_new.pop('geometry')
    df_new.insert(0, 'geometry', geocol)
    # df_new = df_new[df_new['ZCTA5CE20'].isin(list_zipcode_nyc)] # for further filtering
    df_new.dropna(axis=0, subset='geometry', how='any', inplace=True) #need to drop None value rows for the geometry to be simplified below
    df_new["geometry"] = (df_new.to_crs(df_new.estimate_utm_crs()).simplify(level).to_crs(df_new.crs))
    df_new.to_file(output_path, driver='GeoJSON')
    print(f'finished output {output_path}')
    return

