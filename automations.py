import time
import warnings
import numpy as np
import pandas as pd
from streamlit import markdown
from my_apis.excel_functions import DataExtraction
from my_apis.mb_connection import MetabaseConnection
from my_apis.sf_connection import SalesforceConnection

class Automations:
    def __init__(self, mb_credentials:str, sf_credentials:str, new_login:bool=False, logins_are_paths:bool=True) -> None:
        '''
        :param mb_credentials: path to json file with username, password y current-token.
        :param sf_credentials: path to json file with security_token, username, password, domain.
        :param new_login: wether or not to make a new login in metabase. Token lasts 14 days.
        :param logins_are_paths: wether or not the credentials are paths to files or are jsons alredy. Useful when using streamlit application
        '''
        warnings.filterwarnings('error') # Para poder cachar warnings como exceptions.

        self.__mbc = MetabaseConnection(mb_credentials, new_login=new_login, login_is_path=logins_are_paths)
        self.__sfc = SalesforceConnection(sf_credentials, login_is_path=logins_are_paths)
        self.__DATABASE_ID = 6

    def set_database_id(self, database_id:int) -> None:
        '''
        El id de la base de datos de donde se van a sacar los datos de metabase.
        El default es 6, pero en caso de que en algún momento se necesite cambiar lo permito aquí.
        '''
        self.__DATABASE_ID = database_id

    def get_mb_token(self) -> str:
        '''
        Fución para encontrar el current token de metabase.
        '''
        return self.__mbc.SESSION_ID
    
    def get_salesforce_connection(self) -> SalesforceConnection:
        '''
        Esta función regresa la conexción a Salesforce,
        es útil cuándo se quiere hacer otras cosas que no están necesariamente aquí, pero la ventaja es que ya hice
        toda la conexión una sola vez.

        :return: SalesforceConnection
        '''
        return self.__sfc
    
    def get_metabase_connection(self) -> MetabaseConnection:
        '''
        Esta función regresa la conexción a Metabase,
        es útil cuándo se quiere hacer otras cosas que no están necesariamente aquí, pero la ventaja es que ya hice
        toda la conexión una sola vez.

        :return: MetabaseConnection
        '''
        return self.__mbc

    def __change_values(self, df:pd.DataFrame, sf_field:str, verbose:bool=False, sleep_time:int=0, print_every:int=1, object_type:str='Account') -> list:
        '''
        Esta función recibe un dataframe con índices (salesforce ids) y una sola columna con el valor a cambiar.
        
        :param df: df con el foramto especificado
        :param sf_field: el nombre del campo en salesforce a cambiar
        :param object_type: el tipo de objeto en salesforce que se modificará
        '''
        if print_every <= 0: print_every = 1
        sfc = self.__sfc

        if verbose: markdown(f'Registros a cambiar: **{df.size}**')
        i = 1
        errors = []
        for row in df.itertuples():
            sf_id, value = row
            try:
                sfc.update_record(object_type, sf_id, {sf_field:value})
            except:
                if verbose: print(sf_id, value)
                errors.append(row)
            if verbose and i % print_every == 0: print(f'{i}/{df.size}')
            i += 1
            time.sleep(sleep_time)
        return errors
    
    def __update_values(self, df:pd.DataFrame, sf_id:str, verbose:bool=False, print_every:int=1, object_type:str='Account') -> list:
        '''
        Esta función actualiza los valores para un solo MP.

        :param df: pandas dataframe con índice correspondiente al nombre del campo de salesfore y el valor que se cambia´ra. A diferencia de change_values, esta cambia varios valores para un solo MP, mientras que change_values cambia el mismo valor para varios MPs.
        :param sf_id: el id de salesforce del MP que se desea cambiar.
        :param object_type: el tipo de objeto en salesforce que se modificará

        :return: lista con nombres de los errores
        '''
        sfc = self.__sfc
        if print_every <= 0: print_every = 1

        i = 1
        errors = []
        for row in df.itertuples():
            sf_field, value = row
            try:
                sfc.update_record(object_type, sf_id, {sf_field:value})
                if verbose: markdown(f'{sf_field}')
            except:
                if verbose: print(sf_field, value)
                errors.append([sf_field, value])
            if verbose and i % print_every == 0: print(f'{i}/{df.size}')
            i += 1
            
        return errors
    
    def update_main_process(self, mb_query:str, sf_query:str, mb_is_path:bool=True, sf_is_path:bool=True, verbose:bool=False, id_col:str='salesforce_id') -> tuple:
        '''
        Esta función actualiza los account status de los mps en sf.

        :param query: el query que se ejecuta en metabase para encontrar cuántos quotes y wos tiene cada mp
        :param is_path: wether the query is a path to a file containing the query or not
        :param id_col: es necesario cuando se extraen más de 2,000 registros de MB porque es por el que se ordenan para poder sacarlos todos.

        :return: lista con errores, pd.DataFrame con asignaciones distintas
        '''
        mb_data = (
            self
            .__execute_query_in_mb(mb_query, mb_is_path, id_col)
        )

        by_quotes = self.__classify_main_process_by(mb_data, 'total_quotes')
        by_wos = self.__classify_main_process_by(mb_data, 'total_wos')

        iguales = (
            by_wos
            .join(by_quotes, lsuffix='_wos', rsuffix='_quotes', how='outer')
            .query('main_process_wos == main_process_quotes or main_process_quotes.isna() or main_process_wos.isna()')
            .assign(main_process=lambda x: x.main_process_quotes.combine_first(x.main_process_wos))
            [['main_process']]
        )

        sf_data = (
            self
            .__execute_query_in_sf(sf_query, is_path=sf_is_path)
            .set_index('Id')
        )

        diferentes = (
            by_wos
            .join(by_quotes, lsuffix='_wos', rsuffix='_quotes', how='outer')
            .query('main_process_quotes != main_process_wos')
            .dropna()
            .pipe(
                lambda df:
                (
                    mb_data
                    [['salesforce_id', 'mp_name']]
                    .drop_duplicates()
                    .set_index('salesforce_id')
                )
                .join(df, how='right')
            )
            .join(sf_data, how='left')
            .query('main_process_quotes != main_process__c and main_process_wos != main_process__c')
        )

        changes = (
            iguales
            .join(sf_data, how='left')
            .query('main_process != main_process__c')
            .drop(['main_process__c'], axis=1)
        )

        errors = self.__change_values(changes, 'main_process__c', verbose=verbose, print_every=changes.size//10)
        capabilities_errors = self.__match_main_process_capabilities(verbose=verbose)
        errors.extend(capabilities_errors)

        return errors, diferentes
    
    def __match_main_process_capabilities(self, verbose:bool=False) -> list:
        '''
        Esta función hace el match entre main process y los capabilities correspondientes.
        Eg. Si el main process de un mp es Light Fabrication, se asegura de que en capabilities, Light Fabrication al menos tenga General Light Fabrication.
        Esto solo aplica si existe un rubro general para los capabilities correspondientes, es decir, existe alguo que sea General (algo)

        Debido a que necesita muchos inputs, este no es tan generablizable y si necesita cambiar su funcionalidad, se tiene que hacer directo aquí.
        '''
        # Ahora hacemos el match de los main process con los capabilities
        # Estos diccionarios y listas los saqué a mano
        procesos = [
            'other_processes__c',
            'formation_processes__c',
            'materials_processes__c',
            'tooling_processes__c',
            'logistics_processes__c',
            'machining_processes__c',
            'heavy_fab_processes__c',
            'laboratory_processes__c',
            'finishing_processes__c',
            'joining_welding_processes__c',
            'light_fab_processes__c'
        ]

        match_main_process_capabilities = {
            'Machining':'machining_processes__c',
            'Light Fabrication':'light_fab_processes__c',
            'Heavy Fab':'heavy_fab_processes__c',
            'Material Sourcing':'materials_processes__c',
            'Metal Formation':'formation_processes__c',
            'Other':'other_processes__c',
            'Finishing':'finishing_processes__c',
            'Joining and Welding':'joining_welding_processes__c',
            'Logistics':'logistics_processes__c',
            'Laboratory':'laboratory_processes__c'
        }

        match_capability_general_name = {
            'other_processes__c':None,
            'formation_processes__c':'General Metal Formation',
            'materials_processes__c':None,
            'tooling_processes__c':'General Dies and Molds',
            'logistics_processes__c':None,
            'machining_processes__c':'General Machining',
            'heavy_fab_processes__c':'General Heavy Fabrication',
            'laboratory_processes__c':'Laboratory',
            'finishing_processes__c':None,
            'joining_welding_processes__c':None,
            'light_fab_processes__c':'General Light Fabrication'
        }

        query = f'''
        select Id, main_process__c, {', '.join(procesos)}
        from Account
        where main_process__c != null
        '''

        sf_data = (
            self
            .__execute_query_in_sf(query=query, is_path=False)
        )

        # En changes tenemos los siguientes MPs:
        # 1. No tienen procesos asignados (en capabilities) a su main process
        # 2. Su capability correspondiente tiene algún rubro que sea general (eg. General Machining)

        changes = (
            sf_data
            .melt(id_vars=['Id', 'main_process__c'], value_vars=procesos, var_name='capabilities')
            .assign(
                main_process__c=lambda df: df.main_process__c.map(match_main_process_capabilities),
                general_name=lambda df: df.capabilities.map(match_capability_general_name)
            )
            .query('main_process__c == capabilities')
            .dropna(subset=['general_name'])
            .query('value.isna()')
            .set_index('Id')
            [['main_process__c', 'general_name']]
        )

        all_errors = []
        for main_process in changes.main_process__c.unique():
            if verbose: print(f'--- {main_process} ---')
            df_aux = changes.query('main_process__c == @main_process').drop(['main_process__c'], axis=1)
            errors = self.__change_values(df_aux, main_process, verbose=verbose, print_every=(df_aux.size//10)+1)
            all_errors.extend(errors)

        return all_errors


    def __classify_main_process_by(self, df:pd.DataFrame, by:str) -> pd.DataFrame:
        '''
        Esta función asigna el main process con base en el tipo especificado.

        :param df: dataframe con estructra salesforce_id, main_process, total_wos, total_quotes
        '''
        assert by in df.columns, 'The column does not exist'

        classified = (
            df
            .dropna(subset=['main_process'])
            .pivot(index=['salesforce_id'], columns='main_process', values=by)
            .dropna(how='all')
            .assign(main_process=lambda df: df.idxmax(axis=1))
            [['main_process']]
        )
        return classified
    
    def __get_scores_from_excel(self, file, sheet_name) -> tuple:
        '''
        Esta función saca los datos del excel.

        :return: salesforce_id, pd.DataFrame con índice sf_field, value
        '''
        # Necesitamos el match entre los nombres y el nombre de la api
        match_kpi_field = {
            'Responsivo en el correo electrónico y Whatsapp (o equivalente)':'ssc_responsiveness__c',
            'Incluye en sus cotizaciones tiempos de entrega competitivos':'ssc_delivery_times__c',
            'Transparencia y consistencia en métodos de trabajo':'ssc_transparency__c',
            'Costo de materiales, mano de obra, Etc. desglosado':'ssc_cost_breakdown__c',
            'Pizarrón con planeación de la producción':'ssc_production_planning__c',
            'Capacidad instalada / disponible actualizada para Prima':'ssc_available_capacity__c',
            'Participación en RFQs':'ssc_rfq_participation__c',
            'Actualización de proyectos con evidencias (fotos)':'qsc_updates_evidence__c',
            'Quejas de Cliente (Internas - Prima / Externas - Nuestro Cliente)':'qsc_complaints__c',
            'Instrumentos de medición y registros de inspección':'qsc_measuring_instruments__c',
            'Sistema de Gestión de Calidad Implementado':'qsc_quality_systems__c',
            'Rastreabilidad y Certificados de Calidad de Materia Prima':'qsc_materials_trace_cert__c',
            'Costo cercano o debajo de target price':'psc_costs__c',
            'Abierto a negociar o revisar cotizaciones':'psc_willingness_negotiation__c',
            'Documentación Completa para Finanzas':'isc_financial_documentation__c'
        }

        de = DataExtraction(file, sheet_name)

        try:
            row, col = de.find_value('MP ID')[0]
        except IndexError:
            raise(IndexError('No se encontró el ID del MP'))
        
        sf_id = de.get_value_on_cell((row, col+1))

        valores = (
            de
            .extract_data_from_file(index_value='KPI', cols_to_extract=['Calificación'])
            .assign(sf_field=lambda df: df.KPI.map(match_kpi_field))
            .dropna(subset=['sf_field'])
            .drop(['KPI'], axis=1)
            .set_index('sf_field')
        )

        return sf_id, valores
    
    def update_scorecards(self, file, sheet_name=0, verbose:bool=False) -> list:
        '''
        Esta función actualiza los mp scorecards dado un archivo de excel.

        :param file: archivo de excel con el template esperado.
        :param sheet_name: el nombre de la hoja, en caso de existir.
        
        :return: lista con errores.
        '''
        # Encontramos los valores necesarios
        sf_id, valores = self.__get_scores_from_excel(file, sheet_name)

        # Cambiamos los valores
        errors = self.__update_values(valores, sf_id, verbose=verbose)
        return errors

    
    def update_status(self, query:str, is_path:bool=True, verbose:bool=False, id_col:str='salesforce_id') -> list:
        '''
        Esta función actualiza los account status de los mps en sf.

        :param query: el query que se ejecuta en metabase para encontrar las asignaciones incorrectas de status
        :param is_path: wether the query is a path to a file containing the query or not
        :param id_col: es necesario cuando se extraen más de 2,000 registros de MB porque es por el que se ordenan para poder sacarlos todos.

        :return: lista con errores
        '''
        account_status_name = 'Account_Status__c'
        errors = (
            self
            .__execute_query_in_mb(query, is_path, id_col)
            .set_index('salesforce_id')
            .pipe(lambda df: self.__change_values(df, account_status_name, verbose=verbose, print_every=(df.size//10)+1))
        )
        return errors

    
    def update_wos_quotes(self, mb_query:str, sf_query:str, mb_is_path:bool=True, sf_is_path:bool=True, verbose:bool=False, id_col:str='salesforce_id') -> list:
        '''
        Esta función actualiza los wos y quotes en salesforce.

        :param mb_query: el query que se ejecuta en metabase para sacar los wos y quotes
        :param sf_query: el query que se ejecuta en salesforce para sacar los wos y quotes
        :param *_is_path: si el query es un path a un archivo y no el query en sí
        :param id_col: es necesario cuando se extraen más de 2,000 registros de MB porque es por el que se ordenan para poder sacarlos todos.

        :return: lista con los errores que que se hayan encontrado
        '''
        completed_wos_sf = 'Completed_Work_Orders__c'
        completed_quotes_sf = 'Number_of_RFQs_MP_has_quoted__c'
        # 1. Sacamos los wos y quotes de metabase
        mb_data = (
            self
            .__execute_query_in_mb(mb_query, is_path=mb_is_path, id_col=id_col)
            .set_index('salesforce_id')
        )

        # 2. Sacamos los wos y quotes de salesforce
        sf_data = (
            self
            .__execute_query_in_sf(sf_query, is_path=sf_is_path)
            .set_index('Id')
            .rename({'Number_of_RFQs_MP_has_quoted__c':'sf_quotes', 'Completed_Work_Orders__c':'sf_wos'}, axis=1)
            .replace(0, np.nan)
        )

        # 3. Juntamos los datos y encontramos las diferencias
        differences = (
            mb_data
            .join(sf_data, how='left')
            .assign(
                dif_wos=lambda x: x.mb_wos != x.sf_wos,
                dif_quotes=lambda x: x.mb_quotes != x.sf_quotes
            )
        )

        # 4. Actualizamos
        if verbose: 
            print('Updating WOS')
            markdown('Updating **WOs**')
        errors_wos = (
            differences
            .query('dif_wos')
            .dropna(subset=['mb_wos', 'sf_wos'], how='all')
            [['mb_wos']]
            .replace([np.nan, 0], None)
            .pipe(lambda df: self.__change_values(df, completed_wos_sf, verbose=verbose, print_every=df.size//10))  
        )

        if verbose: 
            print('Updating QUOTES')
            markdown('Updating **Quotes**')
        errors_quotes = (
            differences
            .query('dif_quotes')
            .dropna(subset=['mb_quotes', 'sf_quotes'], how='all')
            [['mb_quotes']]
            .replace([np.nan, 0], None)
            .pipe(lambda df: self.__change_values(df, completed_quotes_sf, verbose=verbose, print_every=df.size//10))
        )

        errors_wos.extend(errors_quotes)
        return errors_wos
    
    def update_otif(self, mb_query:str, sf_query:str, mb_is_path:bool, sf_is_path:bool, verbose:bool=False, id_col:str='salesforce_id') -> list:
        '''
        Esta función actualiza los otif en salesforce.
        Se espera que los queries regresen dos columnas: id y otif

        :param mb_query: el query que se ejecuta en metabase para sacar los otifs correctos
        :param sf_query: el query que se ejecuta en salesforce para sacar otif actuales
        :param *_is_path: si el query es un path a un archivo y no el query en sí
        :param id_col: es necesario cuando se extraen más de 2,000 registros de MB porque es por el que se ordenan para poder sacarlos todos.

        :return: lista con los errores que que se hayan encontrado
        '''
        mb_data = (
            self
            .__execute_query_in_mb(query=mb_query, is_path=mb_is_path, id_col=id_col)
            .set_index(id_col)
        )

        sf_data = (
            self
            .__execute_query_in_sf(query=sf_query, is_path=sf_is_path)
            .set_index('Id')
        )

        otif_mb = mb_data.columns[0]
        otif_sf = sf_data.columns[0]

        errors = (
            mb_data
            .join(sf_data, how='outer')
            .query(f'{otif_mb} != {otif_sf}')
            .drop([otif_sf], axis=1)
            .pipe(lambda df: self.__change_values(df, otif_sf, verbose=verbose, print_every=(df.size//10)+1))
        )
        return errors

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
    
    def __execute_query_in_sf(self, query:str, is_path:bool=False) -> pd.DataFrame:
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
        )

        return sf_data
