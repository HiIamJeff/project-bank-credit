
import streamlit as st

from general_utils import generate_available_period
from streamlit_script import load_report, plot_choropleth, generate_st_filter_df


# Initial load
df = load_report(first_load_flag=True)
list_metrics = [c for c in df.columns if c not in ['zip5', 'state', 'covered_major_cities']]
list_period = generate_available_period()


# streamlit
st.set_page_config(layout="wide")

with st.sidebar:
    st.write("## Control Panel")
    metric_input = st.selectbox(
        "Which metrics?", list_metrics, index=list_metrics.index('homebuyers_total')
    )
    period_input = st.selectbox(
        "Which period?", list_period, index=list_period.index('2019/04')
    )


df = load_report(period=period_input)


st.title(f'Project Bank Credit - Monthly Report ({period_input})')
st.write("The zip code records are limited within New York City")

st.plotly_chart(figure_or_data=plot_choropleth(df, metric_input), use_container_width=True)

# Show raw records
st.dataframe(generate_st_filter_df(df))
