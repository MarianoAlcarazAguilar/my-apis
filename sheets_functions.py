import os
import gspread
import numpy as np
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

class SheetsFunctions:
    def __init__(self, spreadsheet_id:str, credentials:str, token:str, sheet_name:str=None) -> None:
        '''
        :param spreadsheet_id: el ide del spreadsheet que se desea trabajar
        :param credentials: path to json file with credentials from GCP project
        :param token: path to token, or path where token will be saved
        '''
        self.__SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        self.__SPREADSHEET_ID = spreadsheet_id
        self.__SHEET_NAME = sheet_name # Este se puede poner después con base en las que haya disponibles

        self.__credentials = self.__load_credentials(credentials, token)
        self.__gc = gspread.authorzie(self.__credentials)

        try:
            self.wb = self.__gc.open_by_url(self.__SPREADSHEET_ID)
        except gspread.exceptions.NoValidUrlKeyFound:
            self.wb = self.__gc.open_by_key(self.__SPREADSHEET_ID)            


        if self.__SHEET_NAME is not None:
            self.ws = self.wb.worksheet(self.__SHEET_NAME)
        
    def get_sheetnames(self) -> list:
        '''
        Regresa una lista con los nombres de las hojas disponilbes en el workbook
        '''
        return self.wb.worksheets()
    
    def set_sheetname(self, sheet_name:str) -> None:
        self.__SHEET_NAME = sheet_name
        self.ws = self.wb.worksheet(self.__SHEET_NAME)
    
    def __load_credentials(self, credentials_path:str, token_path:str) -> Credentials:
        '''
        Esta función carga las credenciales para tener acceso a los datos

        :param credentials: path to json file with credentials from GCP project
        :param token: path to token, or path where token will be saved

        :return: credentials
        '''
        credentials = None

        if os.path.exists(token_path):
            credentials = Credentials.from_authorized_user_file(token_path, self.__SCOPES)
        if not credentials or not credentials.valid:
            if credentials and credentials.expired and credentials.refresh_token:
                credentials.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(credentials_path, self.__SCOPES)
                credentials = flow.run_local_server(port=0)
            with open(token_path, 'w') as token:
                token.write(credentials.to_json())

        return credentials
    
    def write_dataframe(self, df:pd.DataFrame, index:bool=False) -> dict:
        '''
        Esta función escribe el contenido del dataframe al sheets
        Tener cuidado, pues si ya hay valores en las celdas estas serán sobreescritas

        :param df: dataframe a escribir
        :param index: si se desea escribir el index o no
        '''
        if index:
            df.reset_index(inplace=True)

        return self.__ws.update([df.columns.values.tolist()] + df.values.tolist())
    
    def add_record(self, data:dict, index:bool=False) -> dict:
        '''
        Esta función agrega un record a los ya existentes.
        Las columnas de data deben coincidir con las que haya en el archivo
        Agrega el registro al final de los existentes

        :param data: diccionario de la forma {col_name:value}
        '''
        # Leemos el contenido actual de la hoja
        current_data = (
            pd
            .DataFrame(self.__ws.get_all_records())
            .replace({'':None})
        )

        new_data = (
            pd
            .concat((current_data, pd.DataFrame([data])), ignore_index=True)
            .replace({np.NaN:None})
        )
        return self.write_dataframe(new_data, index)
    
    def modify_record(self, id:dict, values:dict) -> dict:
        '''
        Esta función modifica los registros que hagan match con el id especificado.

        :param id: diccionario con las columnas y valores que se usarán para identificar al registro deseado {col_name_i:value_i, col_name_j:value_j}
        :param values: diccionario con los valores que se cambiarán {col_name_k:new_value_k}

        Eg.
        Llamar a la función encontraría los registros cuyo nombre sea mariano, con edad 22 y cambiaría las columnas apellido a 'alcaraz' y sexo a 'h'
        modify_record(
            id={'name':'mariano', 'edad':22},
            values={'apellido':'alcaraz', 'sexo':'h'}
        )
        ''' 
        # Primero hacemos el query para buscar el registro
        query = ' and '.join([f"{col_name} == '{value}'" if isinstance(value, str) else f"{col_name} == {value}" for col_name, value in id.items()])

        # Leemos y filtramos los valores
        df = pd.DataFrame(self.__ws.get_all_records())
        record = df.query(query)
        index = record.index

        # Cambiamos los valores
        for col_name, value in values.items():
            df.loc[index, col_name] = value

        # Guardamos la nueva tabla
        return self.write_dataframe(df, False)


    





