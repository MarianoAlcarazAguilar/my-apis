import re
import json
import collections
import pandas as pd
from simple_salesforce import Salesforce, SFType, SalesforceLogin
from simple_salesforce.exceptions import SalesforceMalformedRequest, SalesforceResourceNotFound

class SalesforceConnection:
    def __init__(self, login_info) -> None:
        """
        :param login_info: información para hacer login, ya sea diccionario, json o path to json file
        """
        # Se tienen los datos para hacer login
        # Y hay dos opciones: nos dan [usuario, contraseña, token] o nos dan [session_id e instance]
        try:
            with open(login_info, 'r') as f:
                login_info = json.load(f)
        except:
            pass

        credentials_login = 'password' in login_info.keys()

        if credentials_login:
            self.__session_id, self.__instance, self.__sf = self.__create_salesforce_connection(**login_info)
        else:
            self.__session_id = login_info["session_id"]
            self.__instance = login_info["instance"]
            self.__sf = Salesforce(**login_info)
            
    def __create_salesforce_connection(self, username:str, password:str, security_token:str, domain:str):
        """
        Esta función se usa para iniciar las sesión de Salesforce.
        """
        session_id, instance = SalesforceLogin(username=username, password=password, security_token=security_token, domain=domain)
        sf = Salesforce(instance=instance, session_id=session_id)
        return session_id, instance, sf
    
    def extract_data(self, query:str) -> pd.DataFrame:
        """
        Esta función se utiliza para hacer un query sobre el objeto Salesforce.
        Returns: Dataframe with extracted data.
        """
        sf = self.__sf

        response = sf.query_all(query=query)
        df = pd.DataFrame(response.get("records")).drop(['attributes'], axis=1)
        return df
    
    def add_record(self, object_type:str, data:dict, handle_exception:bool=False) -> collections.OrderedDict:
        """
        Esta función se usa para agregar registros a un objeto.
        Es importante entender que en SalesForce los registros son objetos, eg. un MP es un objeto de tipo Account.

        Por ahora no se hace control de la excepción por defecto pues muchas funciones dependen de ello.
        En un futuro, se espera que sea el funcionamiento por defecto (handle_expeption = True)

        :param object_type: tipo de objeto a crear en Salesforce. Eg. Account, Contact, Product2
        :param data: diccionario con los campos necesarios para crear el objeto y los que se le quieran agregar
        :param handle_excpetion: si se quiere cuidar que no haya problemas en la ejecución. Eventualmente este será el funcionamiento por defecto
        """
        if not handle_exception:
            sf_object = SFType(object_type, self.__session_id, self.__instance)
            response = sf_object.create(data)
            return response
        else:
            try:
                sf_object = SFType(object_type, self.__session_id, self.__instance)
                response = sf_object.create(data)
            except SalesforceMalformedRequest as error:
                response = collections.OrderedDict({
                    'errors':[error.content[0]['message']],
                    'id':None,
                    'success':False
                })
            return response

        
    
    def delete_record(self, object_type:str, record_id:str):
        """
        Esta función elimina un objeto del tipo especificado.
        Tener cuidado con el uso de esta función, que no sé que tan fácil sea recuperar registros borrados.
        """
        sf_object = SFType(object_type, self.__session_id, self.__instance)
        response = sf_object.delete(record_id=record_id)
        return response
    
    def update_record(self, object_type:str, record_id:str, data:dict):
        """
        Esta función actualiza un objeto dado su id y los datos necesarios.
        """
        sf_object = SFType(object_type, self.__session_id, self.__instance)
        response = sf_object.update(record_id=record_id, data=data)
        return response
    
    def get_available_fields(self, object_type:str, get_all_columns:bool=False) -> pd.DataFrame:
        '''
        Esta función permite extraer los fields disponibles de consulta para el objeto especificado
        '''
        sf_object = SFType(object_type, self.__session_id, self.__instance)
        object_metadata = sf_object.describe().get('fields') # Si cambiamos fields por otra cosa podemos traer en realidad cualquier metadata
        df = pd.DataFrame(object_metadata)

        if get_all_columns: return df
        return df[['name']].rename({'name':'available_fields'}, axis=1)
    
    def get_picklist_values(self, object_type:str, field_name:str) -> pd.DataFrame:
        '''
        Esta función sirve para sacar los available picklist values de un field.

        :param object_type: el tipo de objeto que se buscará
        :param field_name: el field dentro del objeto que se desea

        :return: pd.DataFrame con los picklist values, None si no es picklist values
        '''
        all_metadata = self.get_available_fields(object_type, get_all_columns=True)
        try:
            pick_values = all_metadata.query(f'name == "{field_name}"').picklistValues.values[0]
            list_values = [value.get('label') for value in pick_values]
            return pd.DataFrame(list_values, columns=['picklist_values'])
        except IndexError:
            return None
        
class SalesforceFunctions:
    '''
    Esta clase tiene funciones genéricas que sirven para en (?)
    '''
    def __init__(self, sfc:SalesforceConnection) -> None:
        self.sfc = sfc

    def change_values(self, df:pd.DataFrame, sf_field:str, verbose:bool=False, print_every:int=1, object_type:str='Account') -> list:
        '''
        Esta función recibe un dataframe con índices (salesforce ids) y un solo valor
        sf_field es el nombre del field en salesforce que se quiere cambiar

        Regresa una lista con los valores que no se haya podido actualizar
        '''
        sfc = self.sfc

        i = 1
        errors = []
        for row in df.itertuples():
            sf_id, value = row
            try:
                sfc.update_record(object_type, sf_id, {sf_field:value})
            except SalesforceMalformedRequest as e:
                if verbose: 
                    print(f'Request is malformed {sf_id}')
                errors.append(row)
            except SalesforceResourceNotFound:
                if verbose: print(f'Id not found: {sf_id}')
                errors.append(row)
            if verbose and i % print_every == 0: print(f'{i}/{df.size}')
            i += 1
        return errors

    def change_multiple_values(self, df:pd.DataFrame, verbose:bool=False, print_every:int=1, object_type:str='Account') -> list:
        '''
        Esta función recibe un dataframe con índices (salesforce ids) y columnas correspondientes a los campos a cambiar
        
        :param df: dataframe con formato especificado

        :return: lista con errores
        '''
        sfc = self.sfc
        dictionary = df.transpose().to_dict()
        i = 1
        errors = []
        for sf_id, data_dict in dictionary.items():
            try:
                sfc.update_record(object_type, sf_id, data_dict)
            except SalesforceMalformedRequest as e:
                if verbose: 
                    print(f'Malformed Request for {sf_id}')
                    print(e)
                errors.append(sf_id)
            except SalesforceResourceNotFound:
                if verbose: print(f'Id not found: {sf_id}')
                errors.append(sf_id)
            if verbose and i % print_every == 0: print(f'{i}/{df.shape[0]}')
            i += 1
        return errors

    def add_related_records(self, df:pd.DataFrame, add_type:str, related_index_name:str, constant_values:dict=None, verbose:bool=True, print_every:int=1) -> list:
        '''
        Esta función sirve para crear nuevos registros de objetos que estén relacionados con otro objeto.
        IMPORTANTE: NO VERIFICA QUE EXISTAN DUPLICADOS

        :param df: dataframe con los datos del nuevo objeto con el siguiente formato:
            - index -> el salesforce id del objeto relacionado
            - columnas -> los parámetros del nuevo objeto. Los nombres de las columnas deben coincidir con aquellos de sf
        :param add_type: el tipo de objeto a crear
        :param related_index_name: el nombre del id identificador del objeto relacioando. Eg. AccountId, Account__c, account__c
        :param constant_values: variables que sean iguales para todos los nuevos objetos. Eg. {'type_address__c':'Warehouse'}
        
        :return: lista con los errores encontrados

        Eg. add_related_records(sfc, df, add_type='address__c', related_index_name='Account__c', constant_values={'type_address__c':'Warehouse'})
        '''
        sfc = self.sfc
        if print_every <= 0: print_every = 1
        columns = df.columns
        i = 1
        errors = []
        for row in df.itertuples():
            
            # Creamos el diccionario para crear el nuevo objeto
            sf_index = row[0]
            data_dict = {related_index_name:sf_index}
            if constant_values is not None: data_dict.update(constant_values)
            
            for index, column in enumerate(columns, start=1):
                data_dict[column] = row[index]
        
            try:
                sfc.add_record(add_type, data_dict)
            except SalesforceMalformedRequest as e:
                if verbose: print(e)
                errors.append(row)
            except SalesforceResourceNotFound:
                if verbose: print(f'Could not find {index}')
                errors.append(row)
            if verbose and i % print_every == 0: print(f'{i}/{df.shape[0]}')
            i += 1
        return errors

    def add_multiple_records(self, df:pd.DataFrame, add_type:str, verbose:bool=False, print_every:int=1) -> list:
        '''
        Esta función sirve para dar de alta múltiples registros a la vez desde un dataframe de pandas.

        :param df: dataframe con un registro por fila; los nombres de las columnas deben coincidir con los nombres de la api de salesforce
        :param add_type: el tipo de objeto del cual se crearán nuevos registros

        :return: lista con errores
        '''
        sfc = self.sfc
        if print_every <= 0: print_every = 1
            
        i = 1
        errors = []
        aux_dict = df.transpose().to_dict()
        
        for _, data_dict in aux_dict.items():
            try:
                sfc.add_record(add_type, data_dict)
            except SalesforceMalformedRequest as e:
                if verbose: 
                    print(f'Malformed Request for {data_dict}')
                    print(e)
                errors.append(data_dict)
            except SalesforceResourceNotFound:
                if verbose: print(f'Id not found: {data_dict}')
                errors.append(data_dict)
            if verbose and i % print_every == 0: print(f'{i}/{df.shape[0]}')
            i += 1
        return errors

    def get_record_type_id(self, sobject:str, record_type_name:str):
        '''
        Encontrar el record type id con base en el nombre del objeto dueño y del nombre que tiene

        :param sfc: conexión exitosa a Salesforce
        :param sobject: el tipo de objeto que se busca. Eg. Account, Prouct2, address__c
        :param record_type_name: el nombre del record type. Eg. Manufacturing, Raw Materials Partner

        :return: el id o None, dependiendo si se encuntra o no
        '''
        sfc = self.sfc
        try:
            query = f'''
            select Id
            from RecordType
            where SobjectType = '{sobject}' and Name = '{record_type_name}'
            '''
            rt_id = sfc.extract_data(query).Id[0]
            return rt_id
        except IndexError:
            return None
        
    def __build_query(self, specs:dict) -> str:
        '''
        Esta función construye el query para Salesforce a partir de las especificaciones 
        dadas en un diccionario con la siguiente estructura.

        {
            'sobject':str,
            'fields':[lista con strings de los campos a extraer],
            'filters':[lista con diccionarios de los filtros a aplicar; ver formato de filtros],
            'relation_field':'str' -> en caso de que sea un objeto relacionado al objeto principal. Debe ir acompañado del param source_id
        }

        Formato de los filtros:
        {
            'kind':uno de los siguientes 'str' o 'num', Para 'str' se agregan comillas al where, para num no se agregan.
            'field':str, 
            'condition':str
        }

        :return: query para ejecutarse en salesforce
        '''
        # Agregamos el field relacionado a los fields que hay que extraer en caso de que no venga
        if 'relation_field' in specs and specs['relation_field'] not in specs['fields']:
            specs['fields'].append(specs['relation_field'])

        if 'filters' in specs:
            where_clause = 'where '
            conds = []
            for condition in specs['filters']:
                kind = condition.get('kind')
                if kind == 'str':
                    str_con = f"{condition['field']} = '{condition['condition']}'"
                elif kind == 'num':
                    str_con = f"{condition['field']} {condition['condition']}"
                conds.append(str_con)
            where_clause += ' and '.join(conds)
        else:
            where_clause = ''

        query = f'''
        select Id, {', '.join(specs['fields'])}
        from {specs['sobject']}
        {where_clause}
        '''

        # Regresamos el query terminado
        return query
    
    def __build_df(self, specs:dict, source_id_name:str=None) -> pd.DataFrame:
        '''
        Esta función ejecuta el query dado y cambia los nombres para que se pueda juntar con
        los datos del source.

        Si se da relation_field, se tiene que dar source_id_name
        '''
        sfc = self.sfc
        query = self.__build_query(specs)

        # Ejecutamos el query y lo acomodamos como debe de ser con base en los inputs de specs
        id_name = re.sub('(__s$|__c$)', '', specs['sobject']).replace('__', '_') + '_id'
        
        if source_id_name is None:
            data = (
                sfc
                .extract_data(query)
                .rename({'Id':id_name}, axis=1)
                .rename(lambda x: re.sub('(__s$|__c$)', '', x.lower()).replace('__', '_'), axis=1)
            )
            output = (id_name.lower(), data)
        else:
            data = (
                sfc
                .extract_data(query)
                .rename({'Id':id_name, specs['relation_field']:source_id_name}, axis=1)
                .rename(lambda x: re.sub('(__s$|__c$)', '', x.lower()).replace('__', '_'), axis=1)
            )
            output = data
        return output
    
    def extract_from_specifications(self, dictionary:dict, how_join:str='left') -> pd.DataFrame:
        '''
        Esta función crea las relaciones necesarias y regresa el dataframe final
        con base en el input del usuario dado en un diccionario con la siguiente
        estructrua:

        Formato de diccionario para build_query:

        {
            'sobject':str,
            'fields':[lista con strings de los campos a extraer],
            'filters':[lista con diccionarios de los filtros a aplicar; ver formato de filtros],
            'relation_field':'str' -> en caso de que sea un objeto relacionado al objeto principal. Debe ir acompañado del param source_id
        }

        Formato de los filtros:
        {
            'kind':uno de los siguientes 'str' o 'num', Para 'str' se agregan comillas al where, para num no se agregan.
            'field':str, 
            'condition':str
        }

        Formato para el diccionario de specifications
        {
            'source':diccionario con las especificaciones de la función de build_query,
            'related': lista con diccionarios con las mismas especificaciones
        }
        :param how_join: {'inner', 'left', 'right', None}. Si es None, regresa las tablas completas sin juntar
        :return: dataframe con las conexiones especificadas
        '''
        source_id_name, source_data = self.__build_df(dictionary['source'])

        if 'related' not in dictionary: return source_data 
        
        dfs = [self.__build_df(relation, source_id_name) for relation in dictionary['related']]

        if how_join is None: return source_data, dfs
        
        # Y ahora juntamos todos los dfs en uno solo
        for df in dfs:
            source_data = source_data.merge(df, on=source_id_name, how=how_join)
        return source_data