import os
import pandas as pd
import tradingeconomics as te
# from utils.api_handler import TradingEconomicsLoad

# Load credentials from Trading Economics
# credentials = TradingEconomicsLoad('TE_CREDS')



class DataProcessor:
    '''Data processing'''
    def __init__(self, country_name: str, data_dir: str = 'data'):
        self.country_name = country_name.lower()
        self.data_dir = data_dir
        # Create the path if not exist
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
    
    '''Create the path to save or load the Parquet file.'''
    def _get_parquet_path(self, dataset_name: str) -> str:
        return os.path.join(self.data_dir, f"{self.country_name}_{dataset_name}.parquet")
    
    '''If an Parquet file does not exist get None else load it'''
    def _load_from_parquet(self, dataset_name: str) -> pd.DataFrame:
        file_path = self._get_parquet_path(dataset_name)
        if os.path.exists(file_path):
            return pd.read_parquet(file_path)
        return None
    
    '''Save the DataFrame in Parquet format'''
    def _save_to_parquet(self, dataset_name: str, data: pd.DataFrame):
        file_path = self._get_parquet_path(dataset_name)
        data.to_parquet(file_path, index=False)
    
    '''Data from TraingEconomics API'''
    # getIndicatorData
    # Gets the indicators data from the Parquet or API
    def get_indicators_data(self):
        dataset_name = "indicators"
        # Intentar cargar desde Parquet
        data = self._load_from_parquet(dataset_name)
        if data is not None:
            return data
        # Si no existe, hacer la solicitud a la API
        try:
            data = te.getIndicatorData(country=self.country_name)
            df = pd.DataFrame(data)
            self._save_to_parquet(dataset_name, df)
            return df
        except Exception as e:
            raise ValueError(f"Error getting indicators data: {e}")
    
    # getHistoricalData
    # Gets historical data from Parquet or API
    def get_historical_data(self, indicator_name: str):
        if not indicator_name:
            raise ValueError("Indicator name required, please provide a valid indicator name, eg. 'inflation_rate'.")
        dataset_name = f"historical_{indicator_name}"
        # Intentar cargar desde Parquet
        data = self._load_from_parquet(dataset_name)
        if data is not None:
            return data
        # Si no existe, hacer la solicitud a la API
        try:
            data = te.getHistoricalData(country=self.country_name, indicator=indicator_name)
            df = pd.DataFrame(data)
            self._save_to_parquet(dataset_name, df)
            return df
        except Exception as e:
            raise ValueError(f"Error getting historical data: {e}")
    
    # getWBCategories
    # Gets all categories of World Bank from Parquet or API
    def get_wb_categories(self, category_name_value=None):
        """Obtiene categorías del Banco Mundial desde la API o Parquet."""
        dataset_name = "wb_categories"
        # Intentar cargar desde Parquet
        data = self._load_from_parquet(dataset_name)
        if data is not None:
            return data
        # Si no existe, hacer la solicitud a la API
        try:
            data = te.getWBCategories(category=category_name_value)
            df = pd.DataFrame(data)
            self._save_to_parquet(dataset_name, df)
            return df
        except Exception as e:
            raise ValueError(f"Error getting world bank categories data: {e}")
    
    # getWBCountry
    # Gets data of World Bank by country using API or Parquet
    def get_wb_countrydata(self):
        dataset_name = "wb_country"
        # Intentar cargar desde Parquet
        data = self._load_from_parquet(dataset_name)
        if data is not None:
            return data
        # Si no existe, hacer la solicitud a la API
        try:
            data = te.getWBCountry(country=self.country_name)
            df = pd.DataFrame(data)
            self._save_to_parquet(dataset_name, df)
            return df
        except Exception as e:
            raise ValueError(f"Error getting country data from world bank: {e}")
    
    # getWBHistorical
    # Gets historical World Bank Data data from Parquet or API
    def get_wbhistorical_data(self, series_name: str):
        if not series_name:
            raise ValueError("Indicator name required")
        dataset_name = f"historical_wb_{series_name}"
        # Intentar cargar desde Parquet
        data = self._load_from_parquet(dataset_name)
        if data is not None:
            return data
        # Si no existe, hacer la solicitud a la API
        try:
            data = te.getWBHistorical(series_code=series_name)
            df = pd.DataFrame(data)
            self._save_to_parquet(dataset_name, df)
            return df
        except Exception as e:
            raise ValueError(f"Error getting historical World Bank Data data: {e}")
    
    # getCmtCountry
    # Data from Comtrade by country        
    def get_cmt_countrydata(self):
        dataset_name = "cmt_country"
        # Intentar cargar desde Parquet
        data = self._load_from_parquet(dataset_name)
        if data is not None:
            return data
        # Si no existe, hacer la solicitud a la API
        try:
            data = te.getCmtCountry(country=self.country_name)
            df = pd.DataFrame(data)
            self._save_to_parquet(dataset_name, df)
            return df
        except Exception as e:
            raise ValueError(f"Error getting country data from Comtrade: {e}")
    
    # getCmtHistorical
    # Historical data from comtrade 
    def get_cmthistorical_data(self, symbol_name: str):
        if not symbol_name:
            raise ValueError("Symbol name required")
        dataset_name = f"historical_cmt_{symbol_name}"
        # Intentar cargar desde Parquet
        data = self._load_from_parquet(dataset_name)
        if data is not None:
            return data
        # Si no existe, hacer la solicitud a la API
        try:
            data = te.getCmtHistorical(symbol=symbol_name)
            df = pd.DataFrame(data)
            self._save_to_parquet(dataset_name, df)
            return df
        except Exception as e:
            raise ValueError(f"Error getting historical Cometrade data from this country: {e}")