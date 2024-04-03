import os
import pandas as pd
from datetime import datetime, timezone
from my_apis.sf_connection import SalesforceConnection
from my_apis.mb_connection import MetabaseConnection

class MPsFinder:
    '''
    Esta clase tiene toda la funcionalidad necesaria para busar MPs con base en productos y estados seleccionados
    '''
    def __init__(self, sfc:SalesforceConnection, mbc:MetabaseConnection) -> None:
        self.__sfc = sfc
        self.__mbc = mbc
        self.__DATABASE_ID = 6
        self.__catalogue = self.__load_catalogue()
        self.__states = self.__load_states()
        self.__mps = self.__load_mps(interval_days=30)
        self.__mps_db = self.__load_mps_db()

    def get_product_catalgue(self) -> pd.DataFrame:
        return self.__catalogue
    
    def get_mps_db(self) -> pd.DataFrame:
        return self.__mps_db
    
    def get_states(self) -> pd.DataFrame:
        return self.__states
    
    def get_mps(self) -> pd.DataFrame:
        return self.__mps

    def __execute_query_in_sf(self, query:str, is_path:bool=False, rename_output:dir={}) -> pd.DataFrame:
        '''
        Esta función ejecuta un query en salesforce y regresa los resultados en un dataframe.
        '''
        if is_path:
            with open(query, 'r') as f:
                query = f.read()

        sf_data = (
            self
            .__sfc
            .extract_data(query)
            .rename(rename_output, axis=1)
        )
        
        return sf_data
    
    def __execute_query_in_mb(self, query:str, is_path:bool=False, id_col:str=None) -> pd.DataFrame:
        '''
        Esta función ejectua un query en metabase y regresa los resultados en un dataframe.

        :param query: el query a ejecutar, puede ser el path a un sql file.
        :param is_path: wether or not to load the query from the specified file 
        :param id_col: id_col es necesario cuando se extraen más de 2,000 registros de MB porque es por el que se ordenan para poder sacarlos todos.
        '''
        if is_path:
            with open(query, 'r') as f:
                query = f.read()

        try:
            mb_data = (
                self
                .__mbc
                .query_data(query, database_id=self.__DATABASE_ID)
            )
        except UserWarning:
            assert id_col is not None, "Es necesario especificar id_col"
            mb_data = (
                self
                .__mbc
                .query_more_data(query, database_id=self.__DATABASE_ID, id_col=id_col)
            )

        return mb_data

    def __load_catalogue(self) -> pd.DataFrame:
        '''
        Esta función saca el catálogo de productos de raw materials de Salesforce.

        :return: catálogo en df
        '''
        catalogue = self.__execute_query_in_sf(
            query='queries/products_catalogue.sql', 
            is_path=True, 
            rename_output={'Id':'product_id', 'Name':'product_name', 'Family':'product_family', 'rm_material__c':'material'}
        )
        return catalogue
    
    def __load_mps(self, interval_days:int=30) -> pd.DataFrame:
        '''
        Esta función carga la lista de MPs y sus nombres.

        :param interval_days: el número de días a considerar para buscar los quotes y wos que los MPs han hecho.
        '''
        mps = self.__execute_query_in_sf(
            query='queries/mps_names.sql',
            is_path=True,
            rename_output={'Id':'mp_id', 'Name':'mp_name', 'Account_Status__c':'status', 'supply_chain_category__c':'mp_type', 'global_score__c':'score'}
        )

        docs_on_interval = self.__load_docs_on_interval(interval_days=interval_days)

        mps = (
            mps
            .merge(docs_on_interval, on='mp_id', how='left')
            .fillna({'quotes':0, 'wos':0})
            .astype({'quotes':int, 'wos':int})
        )

        return mps
    
    def __date_on_interval(self, date:pd.DatetimeIndex, n_days:int=30) -> bool:
        '''
        Esta función verifica si la fecha dada está en el intervalo de días especificado a partir del día de hoy.

        :param date: fecha a evaluar
        :param n_days: número de días en el intervalo a considerar

        :return: booleano si la fecha está o no en ese intervalo
        '''
        fecha_actual = datetime.now(timezone.utc)
        diferencia = (fecha_actual - date).days
        return abs(diferencia) <= n_days
    
    def __load_docs_on_interval(self, interval_days:int=30) -> pd.DataFrame:
        '''
        Esta función carga el número de quotes y working orders que los MPs han tenido en el intervalo de tiempo especificado.

        :param interval_days: número de días hacia atrás a partir de hoy a considerar.

        :return: dataframe con columnas mp_id (sf id for accounts), qutoes, wos correspondiente al número de documentos en el intervalo
        '''
        docs_data = self.__execute_query_in_mb(
            query='queries/docs_on_interval.sql',
            is_path=True,
            id_col='doc_id'
        )

        docs_on_interval = (
            docs_data
            .assign(
                doc_date=lambda x: pd.to_datetime(x.doc_date, format='ISO8601'),
                on_interval=lambda df: df.doc_date.apply(lambda x: self.__date_on_interval(x, interval_days))
            )
            .query('on_interval')
            .pivot_table(
                index='mp_id',
                columns='tipo',
                values='doc_id',
                aggfunc='count',
                fill_value=0
            )
            .reset_index()
        )
        return docs_on_interval

    def __load_states(self) -> pd.DataFrame:
        '''
        Esta función carga los estados y sus códigos

        :return: pd.DataFrame con los estados y sus códigos
        '''
        state_codes = (
            self
            .__execute_query_in_sf(
                query='queries/states.sql',
                is_path=True,
                rename_output={'States__c':'state', 'state_code__c':'state_code', 'Region__c':'region'}
            )
        )
        return state_codes 
    
    def __load_mps_db(self) -> pd.DataFrame:
        '''
        Esta función se encarga de crear un dataframe con los MPs, sus respectivas ubicaciones y productos

        :return: pd.DataFrame
        '''
        existing_products = self.__execute_query_in_sf(
            query='queries/mps_products.sql',
            is_path=True,
            rename_output={'product__c':'product_id', 'account__c':'mp_id'}
        )

        direcciones = self.__execute_query_in_sf(
            query='queries/addresses.sql',
            is_path=True,
            rename_output={'Account__c':'mp_id', 'location__StateCode__s':'state_code'}
        )

        db = (
            direcciones
            .merge(self.__mps, on='mp_id', how='left')
            .merge(self.__states, on='state_code')
            .merge(existing_products, on='mp_id')
            .merge(self.__catalogue, on='product_id')
        )

        return db

    def filter_mps(self, products:list, state:str, show_region_mps:bool, show_quotes:bool, show_wos:bool, show_status:bool, show_type:bool, show_score:bool) -> pd.DataFrame:
        '''
        Esta función busca los mps que hagan match con los filtros especificados.

        :param products: lista con los nombres de los productos
        :param state: string con el nombre del estado que se desea buscar

        :return: dataframe formateado para el display
        '''
        pivot_index = ['mp_name']
        if show_quotes: pivot_index.append('quotes')
        if show_wos: pivot_index.append('wos')
        if show_status: pivot_index.append('status')
        if show_type: pivot_index.append('mp_type')
        if show_score: pivot_index.append('score')
    
        if not show_region_mps:
            search_result = (
                self
                .__mps_db
                .query('product_name in @products')
                .query('state == @state')
                .drop_duplicates(subset=['mp_id', 'product_id'])
                .pivot(index=pivot_index, columns='product_name', values='state')
                .fillna(0)
                .astype(bool)
                .assign(total_products=lambda x: x.sum(axis=1))
                .sort_values('total_products', ascending=False)
                .reset_index()
                .set_index('mp_name')
            )
        else:
            state_region = self.__states.query('state == @state').region.values[0]
            pivot_index.append('state')
            search_result = (
                self
                .__mps_db
                .query('product_name in @products')
                .query('region == @state_region')
                .drop_duplicates(subset=['mp_id', 'product_id'])
                .pivot(index=pivot_index, columns='product_name', values='state')
                .fillna(0)
                .astype(bool)
                .assign(total_products=lambda x: x.sum(axis=1))
                .sort_values(['total_products', 'state'], ascending=False)
                .reset_index()
                .set_index('mp_name')
                .rename({'state':state_region}, axis=1)
            )
        return search_result.drop(['total_products'], axis=1)
    
    def get_contact_info(self, mps:list) -> pd.DataFrame:
        '''
        Dada una lista de nombres de mps, regresa la información de los contactos relacionados.

        :param mps: lista con los nombres de los MPs que se buscan

        :return: pd.DataFrame con los contactos
        '''
        chosen_mps_ids = self.__mps.query('mp_name in @mps').mp_id.values.tolist()
        if len(chosen_mps_ids) == 0: return None

        query = f'''
        select AccountId, LastName, FirstName, Phone, MobilePhone, Email, Title
        from Contact
        where {' or '.join([f"AccountId = '{mp_id}'" for mp_id in chosen_mps_ids])}
        '''

        mps_contacts = (
            self
            .__execute_query_in_sf(query)
            .dropna(subset=['Phone', 'MobilePhone', 'Email', 'Title'], how='all')
            .rename({'AccountId':'mp_id'}, axis=1)
            .merge(self.__mps[['mp_id', 'mp_name']], on='mp_id')
            .set_index('mp_name')
            .drop(['mp_id'], axis=1)
            .dropna(axis=1, how='all')
        )

        return mps_contacts