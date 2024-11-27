import streamlit as st
from utils.data_processor_parquet import DataProcessor
import pandas as pd
import plotly.express as px
import warnings
warnings.filterwarnings("ignore")

# Data directory
data_dir = 'data'

# Title
st.title('Economic Analysis by Country')

# Allowed countries
countries = ['mexico', "new_zealand", 'thailand', 'sweden']
rates = {
    'Sweden':1.13,
    'Thailand':0.029,
    'Mexico':0.048,
    'New Zealand':0.58
}

# Function to manage the cache and process the data
@st.cache_resource
def get_processor(country):
    return DataProcessor(country_name=country, data_dir=data_dir)

# Function to select another country
@st.cache_resource
def getting_metric_data(country, metric):
    multi_processor = get_processor(country)
    return multi_processor.get_historical_data(metric)


# Function to plot one or more countries
def plot_combined_data(metric_name):
    st.subheader(f"{metric_name}")
    historical_data = processor.get_historical_data(metric_name)
    historical_data['country'] = selected_country
    historical_data['DateTime'] = pd.to_datetime(historical_data['DateTime'], format='ISO8601', utc=True)
    historical_data['USD_value'] = historical_data.apply(lambda row:row['Value'] * rates[row['country']], axis=1)
    fig = px.line(historical_data[1:], x='DateTime', y='USD_value', color='country')
    
    if compare_selection:
        for compare_country in compare_selection:
            compare_data = getting_metric_data(compare_country, metric_name)
            compare_data['country'] = compare_country
            compare_data['DateTime'] = pd.to_datetime(compare_data['DateTime'], format='ISO8601', utc=True)
            compare_data['USD_value'] = compare_data.apply(lambda row:row['Value'] * rates[row['country']], axis=1)
            plot_data = pd.concat([historical_data[1:], compare_data[1:]], ignore_index=True)
            fig = px.line(plot_data, x='DateTime', y='USD_value', color='country')
        st.plotly_chart(fig, on_select="rerun", use_container_width=True, theme="streamlit")
    else:
        st.plotly_chart(fig, on_select="rerun", use_container_width=True, theme="streamlit")

# Function to highlight values in a column
def highlight_latest(row):
    if row["LatestValue"] > row["PreviousValue"]:
        return "background-color: #b7f5b0; color: black;"
    else:
        return "background-color: #eb6565; color: black;"
    
# Sidebar for country selection
st.sidebar.title('Parameters')

# Country selection
selected_country = st.sidebar.selectbox(
    'Select country',
    options=[c.title().replace('_', ' ') for c in countries],
    index=0
)
# Country selection to process
if selected_country:
    processor = get_processor(selected_country)
    
    #Fisrt part of the app
    # Show indicators table
    st.subheader("Indicators Data")
    indicators_data = processor.get_indicators_data()
    st.dataframe(indicators_data[['Category','Title','LatestValue','PreviousValue','Source','SourceURL',
                                 'CategoryGroup','Frequency','LatestValueDate','PreviousValueDate','FirstValueDate']].style.apply(lambda row: [highlight_latest(row) for _, row in indicators_data.iterrows()], axis=0, subset=["LatestValue"]),
    column_config={
         'SourceURL': st.column_config.LinkColumn("SourceURL"),
         'LatestValueDate': st.column_config.DateColumn("LatestValueDate"),
         'FirstValueDate': st.column_config.DateColumn("FirstValueDate"),
         'PreviousValueDate': st.column_config.DateColumn("PreviousValueDate")
         },
    hide_index=True
    )
    # Second part
    st.title('Common Indicators')
    
    # Comparing countries
    compare_selection = st.multiselect(
        'Select countries to compare', 
        options=[country.title().replace('_', ' ') for country in countries if country.title().replace('_', ' ') != selected_country]
    )

    # Sidebar Selectbox to plot Common Indicators
    common_selec_ind = st.sidebar.selectbox(
        'Common indicators selection',
        options = ['Wages', 'GDP', 'Population'],
        index=0
    )
    # Plotting the common indicators using the function
    if common_selec_ind:
        plot_combined_data(common_selec_ind)
    # Third part
    # Show historical data
    # Sidebar Selectbox
    st.sidebar.title('Historical Data')
    historical_indicator = st.sidebar.selectbox(
        'Select indicator for historical data',
        options=indicators_data['Category'].unique(),
        index=0
    )
    # Plotting Historical data
    if historical_indicator:
        st.subheader(f"Historical Data for indicator: \n {historical_indicator}")
        # st.subheader(f"{historical_indicator}")
        historical_data = processor.get_historical_data(historical_indicator)
        fig = px.line(historical_data.iloc[2:,1:], x='DateTime', y='Value',
                      labels={"DateTime": "Date"}
                      )
        st.plotly_chart(fig, on_select="rerun")

    #Fourth part
    st.title("World Bank: Country Data")
    wb_country_data = processor.get_wb_countrydata()

    # Sidebar Selectbox World Bank: Country Data
    st.sidebar.title('World Bank: Categories')
    wbdata_box = st.sidebar.selectbox(
        'Select Category for World Bank Data',
        options=wb_country_data['category'].unique(),
        index=0
    )
    # World Bank Data category group dataframe
    if wbdata_box:
        st.subheader(f"Historical Data for group: {wbdata_box}")
        category_wbdata = wb_country_data[wb_country_data['category'] == wbdata_box]
        st.dataframe(
            data = category_wbdata[['title','last','description']],
            hide_index=True
        )
    # Selecting only the keys to show in the selectbox
    if category_wbdata is not None:
        wbdata_dict = category_wbdata.set_index('title')['symbol'].to_dict() 
        symbol = st.selectbox(
            'Select a title to see the assosiated graph',
            options = list(wbdata_dict.keys())
        )
        # Show data of the category from above
        if symbol:
            st.subheader(f"{symbol}")
            wb_historical = processor.get_wbhistorical_data(wbdata_dict[symbol])
            wb_historical['date'] = pd.to_datetime(wb_historical['date'], format='ISO8601', utc=True)
            fig = px.line(wb_historical, x='date', y='value')
            st.plotly_chart(fig, on_select="rerun")
    
    #Fifth part
    # Comtrade Country Data (Table)
    comtrade_data = processor.get_cmt_countrydata()
    st.title("Comtrade: Country Data")
    st.write('👉  10,000 data sampling  👈')
    st.dataframe(comtrade_data[['title', 'country2', 
                                'value','date', 'type']].dropna(), 
                                column_config={
                                    "date": st.column_config.DateColumn("date", format="YYYY")
                                },
                                hide_index=True)
    # Filtering data by None's data in 'category' column and grouping by 'type'
    # Plotting Balance Trade
    balance_type_data = comtrade_data[comtrade_data['category'].isna()].groupby('type').sum().reset_index()
    st.subheader('Balance of Trade')
    st.write('👉  10,000 data sampling  👈')
    fig = px.bar(balance_type_data[['type', 'value']], x='type', y='value',color='type', 
                 labels={"type": "Type of trade",
                         'value': 'Monetary Value (by year)'})
    st.plotly_chart(fig, on_select="rerun")
    
    # Sorting the main comtrade table
    principal_partner = comtrade_data[comtrade_data['category'].isna()].sort_values(ascending=False, by='value')
    st.subheader(
        f'Five major trading partners with {selected_country}'
    )
    st.write('👉  10,000 data sampling  👈')
    
    # Columns
    col1, col2=st.columns(spec=2)

    #Ploting comtrade data by exports/imports; Top five trading parthners
    with col1:
        st.write('Exports ⬆️')
        fig_exp = px.bar(principal_partner[principal_partner['type'] == 'Export'].head(5), 
                     x='country2', y='value', color='country2',
                     labels={"country2": "Country",
                             'value': 'Monetary Value (last year)'})
        st.plotly_chart(fig_exp, on_select="rerun", use_container_width=True, )

    with col2:
        st.write('Imports ⬇️')
        fig_imp = px.bar(principal_partner[principal_partner['type'] == 'Import'].head(5), 
                     x='country2', y='value', color='country2',
                     labels={"country2": "Country",
                             'value': 'Monetary Value (last year)'})
        st.plotly_chart(fig_imp, on_select="rerun", use_container_width=True)
    # Buttons
    with col1:
        ex = st.button(
            'Export Line Plot'
        )
    with col2:
        im = st.button(
            'Import Line Plot'
        )
    
    # Plotting historical data partners  
    if ex:
        st.write('Exports')
        fetch_cmt_symbol = principal_partner[principal_partner['type'] == 'Export'].head(5).reset_index(drop=True)
        fetch_cmt_symbol_dic=fetch_cmt_symbol.set_index('country2')['symbol'].to_dict()
        # st.dataframe(fetch_cmt_symbol)
        all_hist=[]
        for country, symbol in fetch_cmt_symbol_dic.items():
            hist_cmt_partners = processor.get_cmthistorical_data(symbol_name=symbol)
            hist_cmt_partners['country'] = country
            all_hist.append(hist_cmt_partners)
    
        data = pd.concat(all_hist, ignore_index=True)
        fig = px.line(data, x='date', y='value', color='country', 
        labels={
            'country':'Country',
            'date': 'Date',
            'value': 'Monetary Value'
        })
        st.plotly_chart(fig, on_select="rerun", use_container_width=True)

    if im:
        st.write('Imports')
        fetch_cmt_symbol = principal_partner[principal_partner['type'] == 'Import'].head(5).reset_index(drop=True)
        fetch_cmt_symbol_dic=fetch_cmt_symbol.set_index('country2')['symbol'].to_dict()
        all_hist=[]
        for country, symbol in fetch_cmt_symbol_dic.items():
            hist_cmt_partners = processor.get_cmthistorical_data(symbol_name=symbol)
            hist_cmt_partners['country'] = country
            all_hist.append(hist_cmt_partners)

        data = pd.concat(all_hist, ignore_index=True)
        fig = px.line(data, x='date', y='value', color='country', 
        labels={
            'country':'Country',
            'date': 'Date',
            'value': 'Monetary Value'
        })
        st.plotly_chart(fig, on_select="rerun", use_container_width=True)