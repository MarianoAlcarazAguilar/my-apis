import json
import pandas as pd
from simple_salesforce import Salesforce, SFType, SalesforceLogin

class SalesforceConnection:
    def __init__(self, login_info_path, login_is_path:bool=True) -> None:
        """
        :param login_info_path: path to login information in json format with username, password, security_token and domain values
        :param login_is_path: wether or not login credentials is a path to a json file or a json already. Useful for streamlit applications.
        """
        if login_is_path:
            with open(login_info_path, 'r') as f:
                login_info = json.load(f)
        else:
            login_info = login_info_path

        username = login_info["username"]
        password = login_info["password"]
        security_token = login_info["security_token"]
        domain = login_info["domain"]

        self.__session_id, self.__instance, self.__sf = self.__create_salesforce_connection(username, password, security_token, domain)

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
    
    def add_record(self, object_type:str, data:dict):
        """
        Esta función se usa para agregar registros a un objeto.
        Es importante entender que en SalesForce los registros son objetos, eg. un MP es un objeto de tipo Account.
        """
        sf_object = SFType(object_type, self.__session_id, self.__instance)
        response = sf_object.create(data)
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


        

if __name__ == "__main__":
    sf = SalesforceConnection(".login.json")

    # Extraemos los datos de los MPs
    query = """
        select Id, Name, Account_Status__c
        from Account
    """
    extracted_data = sf.extract_data(query)

    # Creamos un nuevo registro
    data = {"Name":"Mariano created this and should be deleted"}
    response = sf.add_record("Account", data)
    created_id = response.get("id")

    # Actualizamos el registro
    update_data = {"Email__c":"mariano@mail.com"}
    response = sf.update_record("Account", created_id, update_data)

    # Eliminamos el registro
    sf.delete_record("Account", created_id)
    