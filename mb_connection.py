import json
import warnings
import requests
import pandas as pd

class MetabaseConnection:
    def __init__(self, login_credentials, new_login:bool=False) -> None:
        '''
        :param login_credentials: path to login information in json format with username, password of metabase.
        :param login_is_path: wether or not login credentials is a path to json file or it's a json already.
            This is useful when using credential that were loaded in a streamlit application.
        '''
        # Intentamos leer los datos (suponiendo que nos dan un json que hay que leer)
        try:
            with open(login_credentials, 'r') as f:
                login_info = json.load(f)
            login_is_path = True
        except:
            login_info = login_credentials
            login_is_path = False

        self.METABASE_DOMAIN = login_info['metabase_domain']
        use_api_key = 'api_key' in login_info.keys()

        if use_api_key:
            self.SESSION_ID = login_info['api_key']
            self.headers = {'x-api-key':self.SESSION_ID}
        else:
            self.SESSION_ID=self.__get_session_token(login_credentials, new_login, login_is_path)
            self.headers = {'X-Metabase-Session': self.SESSION_ID}

    def __get_session_token(self, login_credentials, new_login:bool, login_is_path:bool):
        '''
        Esta función obtiene el session token.
        '''
        if login_is_path:
            with open(login_credentials, 'r') as f:
                login_info = json.load(f)
        else:
            login_info = login_credentials

        if not new_login: return login_info["current-token"]    

        username = login_info["username"]
        password = login_info["password"]
        
        session_id = requests.post(
            f'{self.METABASE_DOMAIN}/api/session',
            json={
                'username': username, 'password': password
            }
        ).json()['id']

        # Sobreescribmos el json con el nuevo token
        login_info['current-token'] = session_id
        if login_is_path:
            with open(login_credentials, 'w') as file:
                json.dump(login_info, file)
            
        return session_id

    def get_database_id(self, database_name:str) -> int:
        '''
        Busca en la lista de bases de datos y devuelve el ID de la base de datos con el nombre dado.

        :param database_name: El nombre de la base de datos a buscar.
        :return: El ID de la base de datos si se encuentra, de lo contrario None.
        '''
        json_data = requests.get(
            url=f'{self.METABASE_DOMAIN}/api/database',
            headers=self.headers
        ).json()

        # Iterar sobre cada base de datos en la lista
        for db in json_data['data']:
            # Verificar si el nombre de la base de datos coincide
            if db['name'] == database_name:
                # Devolver el ID de la base de datos
                return db['id']
        # Si no se encuentra la base de datos, devolver None
        return None
    
    def __create_dataframe_from_json(self, json_data) -> pd.DataFrame:
        '''
        Función que limpia el json y regresa el dataframe limpio.
        '''
        # Extraemos las filas y los nombres de las columnas
        rows = json_data['data']['rows']
        cols = [col['display_name'] for col in json_data['data']['cols']]

        return pd.DataFrame(rows, columns=cols)
    
    def query_data(self, query:str, database_id:int=6, supress_warning:bool=False) -> pd.DataFrame:
        '''
        Esta función recibe un query y lo ejecuta en metabase para extraer los datos deseados.

        :param query: el string del query que se va a extraer.
        :param database_id: el id del database que se va a usar. El default de Prima DWH es 6.

        En caso de querer usar otra database existe la función get_database_id() para encontrarlo.
        '''
        json_data = requests.post(
            url=f'{self.METABASE_DOMAIN}/api/dataset',
            headers=self.headers,
            json={
                'database':database_id,
                'type':'native',
                'native':{
                    'query':query
                }
            }
        ).json()

        df = self.__create_dataframe_from_json(json_data)
        if df.shape[0] == 2_000 and not supress_warning:
            warning_message = '''
    
                WARNING: your query might have more records than what you are seeing. Limit is 2,000

                There is a workaround to this, but the following must be true in order for it to work:
                    - A unique numerical id exists in your columns
                    - Your query must be editable so that a simple where and order by clause are included

                Example
                Suppose your query is something like this: 
                    
                    select Id, salesforce_id 
                    from companies

                Your query works, but you need to edit it so that it ends up like this:

                    select Id, salesforce_id
                    from companies
                    --insert_where_clause_here
                    --insert_order_by_clause_here

                Use the function query_more_data and specifiy the unique id column that will be used.
                Hint: if your query does not apply, try using the with clause in the original query and modify
                the result to select all from the las common table expression.

                    with final_query as (
                        select Id, salesforce_id
                        from companies
                    )
                    select *
                    from final_query
                    --insert_where_clause_here
                    --insert_order_by_clause_here
            '''
            warnings.warn(warning_message)
        return df
    
    def query_more_data(self, query:str, id_col:str, id_col_df:str=None, database_id:int=6, min_value:int=0, where_clause:str='--insert_where_clause_here', order_by_clause:str='--insert_order_by_clause_here') -> pd.DataFrame:
        '''
        Esta función extrae datos pero es útil cuando se necesita sacar tablas con más
        de 2000 rows, pues de otra forma es imposible obtenerlos todos.

        :param query: el query que se buscará, debe tener el formato de dónde se incluirá el where y el order by
        :param id_col: la columna que funciona como identificador, debe ser numérico y único en la columna
        :param id_col_df: el nombre de la columna al extraer los datos, normalmente será igual que id_col, pero si es distinto se puede especificar
        :param min_value: el valor mínimo esperado en la columna de id. Si no se sabe se puede usar -10000 o algo así
        :param where_clause: el string que viene en el query especificando dónde se pondrá el where clause
        :param order_by_clause: el string que viene en el query especificando dónde se pondrá el order by clase
        '''
        if id_col_df is None: id_col_df = id_col
        data_list = []
        max_value = min_value

        i = 0
        while 1:
            # print(i)
            new_query = (
                query
                .replace(where_clause, f"where {id_col} > '{max_value}'")
                .replace(order_by_clause, f'order by {id_col}')
            )
            data = self.query_data(new_query, supress_warning=True, database_id=database_id)
                
            max_value = data[id_col_df].max()
            data_list.append(data)

            if data.shape[0] < 2_000: 
                break
            
            print(max_value)
            i += 1

        return pd.concat(data_list, ignore_index=True)
        
    
    def __get_database_metadata(self, database_id:int):
        '''
        Esta función regresa la metadata de la base de datos especificada.
        '''
        db_metadata = requests.get(
            url=f'{self.METABASE_DOMAIN}/api/database/{database_id}/metadata',
            headers=self.headers
        ).json()  

        return db_metadata
    
    def get_tables_in_database(self, database_id:int=6, as_list:bool=False):
        '''
        Esta función regresa las tablas disponibles dentro de una base de datos.

        :param database_id: id de la base de datos.
        :param as_list: si se desean los nombres en una lista; en caso contrario regresa pd.DataFrame
        '''
        # Sacamos los metadatos de la db que nos dieron
        db_metadata = self.__get_database_metadata(database_id)  

        tables = [table['name'] for table in db_metadata['tables']]# if table['entity_type'] in ['entity/CompanyTable', 'entity/GenericTable']]

        if as_list: return tables
        return pd.DataFrame(tables, columns=['tables'])
    
    def get_columns_in_table(self, table_name:str, database_id:int=6, as_list:bool=False, include_type:bool=False):
        '''
        Esta función regresa las columnas de una tabla dada.

        :param table_name: el nombre de la tabla que se busca
        :param database_id: el id de la base de datos donde se espera esté la tabla
        :param as_list: si es true regresa los datos como lista y no como pd.DataFrame
        :param include_type: si es true regresa además el tipo de dato

        :return: lista, pd.DataFrame o None si la tabla no está en la base de datos
        '''
        db_metadata = self.__get_database_metadata(database_id) 
        tables = db_metadata['tables']

        fields = None

        for table in tables:
            if table['name'] == table_name:
                fields = table['fields']
                break

        # Si fields sigue siendo None significa que el nombre de la tabla no existe
        if fields is None: return None

        if include_type:
            data = [(field['name'], field['database_type']) for field in fields]
            if as_list: return data
            return pd.DataFrame(data, columns=['column', 'type'])
        else:
            columns = [field['name'] for field in fields]
            if as_list: return columns
            return pd.DataFrame(columns, columns=['column'])
        
    # TODO: Encontrar qué queries usan una tabla dada
    # 1. Econtrar el id de la tabla especificada
    def get_table_id(self, table_name:str, database_id:int=6) -> int:
        '''
        Esta función regresa el id de la tabla especificada.

        :param table_name: el nombre de la tabla
        :param database_id: id de la base de datos donde está la tabla
        :return: el id de la tabla; None si no se encontró.
        '''
        db_metadata = self.__get_database_metadata(database_id) 
        tables = db_metadata['tables']

        for table in tables:
            if table['name'] == table_name:
                return table['id']
            
        return None
        

if __name__ == '__main__':
    print('mb_connection.py running as main')

    # Este sirve de ejemplo de caso de uso para las funciones que están arriba
    mb = MetabaseConnection(login_credentials='../.mb_credentials.json')
    
    # Encontramos el id de la base de datos de prima para identificar qué tablas hay
    db_name = 'Prima DWH'
    db_id = mb.get_database_id(db_name)
    tablas = mb.get_tables_in_database(db_id)

    # Nos damos cuenta que queremos los nombres de las compañías con el salesforce_id
    table_name = 'companies'
    columns = mb.get_columns_in_table(table_name=table_name, database_id=db_id, include_type=True)

    query = '''
        select salesforce_id, fiscal_name
        from companies
    '''
    data = mb.query_data(query, database_id=db_id, supress_warning=True) # Esto nos va a dar un warning de que hay más registros

    # Modificamos el query para que pueda sacar todos los registros disponibles
    query = '''
        select salesforce_id, fiscal_name
        from companies
        --where
        --orderby
    '''
    more_data = mb.query_more_data(query, id_col='salesforce_id', database_id=db_id, where_clause='--where', order_by_clause='--orderby')
