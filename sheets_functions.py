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
        self.__gc = gspread.authorize(self.__credentials)

        try:
            self.__wb = self.__gc.open_by_url(self.__SPREADSHEET_ID)
        except gspread.exceptions.NoValidUrlKeyFound:
            self.__wb = self.__gc.open_by_key(self.__SPREADSHEET_ID)            

        self.__ws = None
        if self.__SHEET_NAME is not None:
            self.__ws = self.__wb.worksheet(self.__SHEET_NAME)
        
    def get_sheetnames(self) -> list:
        '''
        Regresa una lista con los nombres de las hojas disponilbes en el workbook
        '''
        return self.__wb.worksheets()

    def get_worksheet(self) -> gspread.worksheet.Worksheet:
        '''
        Esta función regresa el working sheet que se está usando para poder usar todos los 
        métodos que tenga disponible. Esto es para evitar otros problemas.

        Warning: this might be none

        :return: 
        '''
        if self.__ws is None:
            raise 'No work sheet has been specified'
        return self.__ws
        
    def get_current_data(self) -> pd.DataFrame:
        '''
        Esta función extrae la información contenida actualmente en el sheets.

        :return: pd.DataFrame con el contenido, sustitye '' por None
        '''
        try:
            current_data = (
                pd
                .DataFrame(self.__ws.get_all_records())
                .replace({'':None})
            )
        except gspread.exceptions.GSpreadException:
            # Esta exepción ocurre cuando los nombres de las columnas
            # son iguales, lo que evita que funcione get_all_records()
            # print('Unable to read columns due to duplicity of names')

            current_data = (
                pd
                .DataFrame(self.__ws.get_values())
                .replace({'':None})
                .dropna(how='all', axis=1)
                .dropna(how='all', axis=0)
            )
        return current_data
    
    def set_sheetname(self, sheet_name:str) -> None:
        self.__SHEET_NAME = sheet_name
        self.__ws = self.__wb.worksheet(self.__SHEET_NAME)
    
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
    
    def add_record(self, data:dict, index:bool=False) -> None:
        '''
        Esta función agrega un record a los ya existentes.
        Las columnas de data deben coincidir con las que haya en el archivo
        Agrega el registro al final de los existentes

        :param data: diccionario de la forma {col_name:value}
        '''
        # Leemos el contenido actual de la hoja
        current_data = self.get_current_data()

        new_data = (
            pd
            .concat((current_data, pd.DataFrame([data])), ignore_index=True)
            .replace({np.NaN:None})
        )
        
        self.write_dataframe(new_data, index)

    def add_multiple_records(self, data:pd.DataFrame, index:bool=False, drop_duplicates:list=None) -> None:
        '''
        Esta función agrega múltiples registros provenientes de un dataframe

        :param data: dataframe a agregar al final de los registros actuales}
        :param index: si se agrega o no el índice del dataframe que se pasó
        '''
        if index:
            data.reset_index(inplace=True)

        current_data = self.get_current_data()

        new_data = (
            pd
            .concat((current_data, data), ignore_index=True)
            .replace({np.NaN:None})
        )

        if drop_duplicates is not None:
            new_data.drop_duplicates(subset=drop_duplicates, inplace=True, keep='last')

        self.write_dataframe(new_data, index)
        

    
    def modify_record(self, id:dict, values:dict) -> None:
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
        self.write_dataframe(df, False)


    





