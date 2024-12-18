import os
import re
import math
import json
import gspread
import requests
import numpy as np
import pandas as pd
from typing import List, Dict
from datetime import datetime
from requests import Response
from fractions import Fraction
from my_apis.sheets_functions import SheetsFunctions
from my_apis.mb_connection import MetabaseConnection
from simple_salesforce import SalesforceResourceNotFound
from my_apis.sf_connection import SalesforceConnection, SalesforceFunctions


class Ichigo:
    def __init__(self, bearer_token:str, connect_to:str):
        """
        This function handles the necessary functionality to use Ichigo
        with python. Right now it only supports authentication with 
        Bearer token, but future implementation should be able to use
        M2M authentication.

        :param bearer_token: string containing bearer token to login
        :param connect_to: {'production','uat}
        """
        self.__bearer_token = bearer_token

        valid_connect_to_options = {'ichigo', 'uat'}
        if connect_to not in valid_connect_to_options:
            raise ValueError(f'Valid options for connect to: {valid_connect_to_options}')
        
        base_endpoint = "https://api.{connect_to}.prima.ai/graphql/"
        self.__endpoint = base_endpoint.format(connect_to=connect_to)
        self.__headers = {
            'authorization': self.__bearer_token,
            'content-type': 'application/json'
        }

    def create_master_code(
        self,
        code:str, 
        type_:str, 
        order:int,
        grouping_code:str='all'
    ) -> Response:
        """
        This function allows the user to create a master code.

        :param code: the name of the master code.
        :param type_: 
            The category that the master code belongs to. 
            Hay un montón, las de raw materials son las siguientes:
            {
                'RawMaterialCategory', 
                'RawMaterialClassification', 
                'RawMaterialDimension', 
                'RawMaterialFinish', 
                'RawMaterialGrade', 
                'RawMaterialMaterial', 
                'RawMaterialPresentation', 
                'RawMaterialThickness'
            }
        :param grouping_code: it's always 'all'
        :param order: the order that it will be displayed.

        :return: dictionary with response from endpoint.
        """
        json_data = {
            'query': """
                mutation CreateMasterCode($createMasterCode: CreateMasterCode) {
                    createMasterCode(createMasterCode: $createMasterCode) {
                        code
                        type
                    }
                }
            """,
            'variables': {
                'createMasterCode': {
                    'code': code,
                    'type': type_,
                    'groupingCode': grouping_code,
                    'order': order,
                },
            },
            'operationName': 'CreateMasterCode',
        }
        resp = requests.post(self.__endpoint, headers=self.__headers, json=json_data)
        return resp
        
    def create_item(
        self,
        name:str,
        weight:float,
        material:str,
        classification:str,
        grade:str,
        dimensions:List[Dict],
        presentation:str=None,
        finish:str=None,
        dimension:str=None
    ) -> Response:
        """
        This function uploads an item to the cataloge.

        :param name: the name that will be assigned to the item
        :param classification: master codes (mc) that correspond to RawMaterialClassification
        :param material: mc that correspond to RawMaterialMaterial
        :param grade: mc that correspond to RawMaterialGrade
        :param presentation: mc that correspond to RawMaterialPresentation
        :param finish: mc that correspond to RawMaterialFinish
        :param weight: the weight of the item
        :param dimension: específicamente para vigas y canales
        :param dimensions: 
            | List with corresponding dimensions for the type of product that is being uploaded.
            | The available keys for the dictionary are: 
            |     - typeCode {Thickness, Width, Length} (for thickess use mc for RawMaterialThickness)
            |     - unitCode {Meter, Feet, Inch}
            |     - measure
            | plano -> Thickness, Width, Length
            | TODO: ir agregando los requerimientos conforme los vaya dando de alta para que no se me olivde después
        """
        json_data = {
            'query': """
                mutation CreateItemCatalog($createItemCatalogInput: CreateItemCatalogInput!) {
                createItemCatalog(createItemCatalogInput: $createItemCatalogInput) {
                    id
                    sku
                }
                }
            """,
            'variables': {
                'createItemCatalogInput': {
                    'classificationCode': classification,
                    'materialCode': material,
                    'gradeCode': grade,
                    'presentationCode': presentation,
                    'finishCode': finish,
                    'name': name,
                    'weight': weight,
                    'overriteWeight': True,
                    'dimensions': dimensions,
                    'dimension':f'{dimension}'
                },
            },
            'operationName': 'CreateItemCatalog',
        }
        response = requests.post(self.__endpoint, headers=self.__headers, json=json_data)
        return response
        
    def remove_item_catalogue(
        self,
        id:int
    ) -> Response:
        """
        This function removes an item from the catalogue in Ichigo.

        :param id_:id of the item that wants to be removed.

        :return: Response with the information of the item that was deleted.
        """
        json_data = {
            'query': '''mutation RemoveItemCatalog($removeItemCatalogId: Int!) {
                removeItemCatalog(id: $removeItemCatalogId) {
                    id
                    name
                    skuRmt
                    }
                }''',
            'variables': {
                'removeItemCatalogId': id,
            },
            'operationName': 'RemoveItemCatalog',
        }
        
        response = requests.post(self.__endpoint, headers=self.__headers, json=json_data)
        return response
        
    def get_master_code_types(
        self
    ) -> Dict:
        """
        This function retrieves not only the available master codes that are important
        for RawMaterials, but it also gives the available options that each one of them has.

        :return: dictionary with structure {'master_code_type':[options]}
        """
        query = """
        query MasterCodes {
        masterCodes {
            code
            type
        }
        }
        """
        response = requests.post(
            self.__endpoint, 
            headers=self.__headers, 
            json={'query':query}
        )
        aux_dict = (
            pd
            .DataFrame(json.loads(response.text)['data']['masterCodes'])
            .query('type.str.startswith("RawMaterial")')
            .groupby('type')
            .agg(options=pd.NamedAgg('code', list))
            .transpose()
            .to_dict()
        )
        mc = {}
        for k, v in aux_dict.items():
            mc[k] = v['options']
        return mc
        
    def get_max_order_master_codes(
        self
    ) -> Dict:
        """
        Extract the max order of each value of the master codes.
        """
        query = """
        query MasterCodes {
        masterCodes {
            code
            type
            order
            groupingCode
            validationCode
        }
        }
        """
        response = requests.post(
            self.__endpoint, 
            headers=self.__headers, 
            json={'query':query}
        )
        
        max_orders = (
            pd
            .DataFrame(json.loads(response.text)['data']['masterCodes'])
            .groupby('type')
            .agg(max_order=pd.NamedAgg('order', 'max'))
            .reset_index()
            .query('type.str.startswith("RawMaterial")')
            .set_index('type')
            .to_dict()
            ['max_order']
        )

        return max_orders
        
    def map_width_to_master_code(
        self,
        width:str|int,
        skip_conversion:bool=False,
        inverse:bool=False
    ) -> int:
        """
        Calibres y espesores en mastercodes tienen un código numérico.
        Su equivalencia es especialemente difícil de inferir cuando 
        corresponden a calibres, por lo tanto, aquí se tiene una tabla
        que mapea el valor de los espesores a su equivalencia numérica.

        :param width: espesor o calibre, pueden ser como "1
        :param skip_conversion: si se quiere o no que se omita la conversión de fraction to float
            | es útil cuando se sabe que no hay número en width y solo se quiere encontrar la equivalencia del diccionario
        :param inverse: if True, returnsn the original width instead of the master code
        """ 
        equivalencias_calibres = {
            'Cal. 32': 90,
            'Cal. 30': 120,
            'Cal. 29': 135,
            'Cal. 28': 149,
            'Cal. 27': 164,
            'Cal. 26': 179,
            'Cal. 25': 209,
            'Cal. 24': 239,
            'Cal. 23': 269,
            'Cal. 22': 299,
            'Cal. 21': 329,
            'Cal. 20': 359,
            'Cal. 19': 418,
            'Cal. 18': 478,
            'Cal. 17': 538,
            'Cal. 16': 598,
            'Cal. 15': 673,
            'Cal. 14': 747,
            'Cal. 13': 897,
            'Cal. 12': 1046,
            'Cal. 11': 1196,
            'Cal. 10': 1345,
            'Cal. 9': 1495,
            'Cal. 8': 1644,
            'Cal. 7': 1443,
            'Cal. 6': 1620,
            'Cal. 5': 1819,
            'Cal. 4': 2043,
            'Cal. 3': 2294,
            'XXS':1, # para cédulas
            'X':5 # para cédulas
        }

        def get_key(val):
            for key, value in equivalencias_calibres.items():
                if val == value:
                    return key
            return val / 10_000
        
        if inverse:
            if type(width) != int:
                raise ValueError('Invalid value for master code')
            return get_key(width)
        
        def __fraction_to_float(original_value:str) -> str:
            value = str(original_value)
            value = value.lower().replace('-', ' ')
            if 'c' in value: return original_value # para evitar limpiar calibres
            value = re.sub(r"[^0-9/\. ]", "", value).strip()
            fractions = [float(Fraction(frac)) for frac in value.split(' ')]
            return int(sum(fractions) * 10_000)
            
        if skip_conversion:
            master_code = width
        else:
            master_code = __fraction_to_float(width)
        
        # Si el master_code es igual al width es porque corresponde a un calibre
        # Y por lo tanto, hacer la conversión tal cual no va a funcionar.
        if master_code != width: return master_code

        # Si estamos aquí, significa que width es un calibre.
        # Hay que hacer uso de tabla de conversión.
        
        if width not in equivalencias_calibres:
            raise ValueError(f"Invalid value for width: {width}")
        return equivalencias_calibres[width]
    
    def load_items_catalog(
        self
    ) -> pd.DataFrame:
        """
        This function extracts the items from ichigo.
        It is better than going to Metabase because the other one doesn't work basically.
        """
        json_data = {
            'query': """
                query ItemsCatalog {
                itemsCatalog {
                    id
                    erpId
                    name
                    sku
                    skuRmt
                    weight
                    unitCode
                    materialCode
                    classificationCode
                    gradeCode
                    presentationCode
                    finishCode
                    categoryCode
                    schedule
                    dimension
                    pricePerKg
                    dimensions {
                    typeCode
                    unitCode
                    measure
                    }
                }
                }
            """,
            'variables': {},
            'operationName': 'ItemsCatalog',
        }
        response = requests.post(self.__endpoint, headers=self.__headers, json=json_data)
        return pd.DataFrame(json.loads(response.text)['data']['itemsCatalog'])

    def build_dimensions(
        self,
        family:str,
        thickness:str=None,
        width_unit:str=None,
        width_value:float|int=None,
        length_unit:str=None,
        length_value:float|int=None,
        wall_length:float|int=None,
        wall_width:float|int=None,
        A:float|int=None,
        B:float|int=None,
        C:float|int=None,
        D:float|int=None,
        kg_m:float|int=None,
        depth:float|int=None,
        diameter:float|int=None,
        cedula:float|int=None
    ) -> List[Dict]:
        """
        This function handles the creation of a list with the corresponding values
        to have the necessary dimenions.

        :param family: what type of item it is, to ensure that the necessary values are available
        :return: list of dictionary ready to send to Ichigo
        """
        necessary_values = {
            'plano':[thickness, width_unit, width_value, length_unit, length_value],
            'perfil':[thickness, wall_length, wall_width, length_unit, length_value],
            'polin':[thickness, length_unit, length_value, A, B],
            'viga-canal':[length_unit, length_value, kg_m, depth],
            'largo-solido':[thickness, length_unit, length_value],
            'tuberia':[thickness, length_unit, length_value, diameter] # Nota: cedula no es obligatorio; se guarda en "Depth"
        }
        
        def validate_inputs() -> bool:
            no_none = [value for value in necessary_values[family] if value not in (None, np.nan)]
            return len(no_none) == len(necessary_values[family])

        if not validate_inputs():
            raise ValueError('Unable to build dimensions with inputs')

        dimensions = []
        if thickness and thickness in necessary_values[family]:
            dimension = {'typeCode': 'Thickness', 'unitCode': None, 'measure': self.map_width_to_master_code(thickness)}
            dimensions.append(dimension)

        if width_value and width_value in necessary_values[family]:
            dimension = {'typeCode': 'Width', 'unitCode': width_unit, 'measure': width_value}
            dimensions.append(dimension)

        if length_value and length_value in necessary_values[family]:
            dimension = {'typeCode': 'Length', 'unitCode': length_unit, 'measure': length_value}
            dimensions.append(dimension)

        if wall_length and wall_length in necessary_values[family]:
            dimension = {'typeCode': 'Wall length', 'unitCode': 'Inch', 'measure': wall_length}
            dimensions.append(dimension)
            
        if wall_width and wall_width in necessary_values[family]:
            dimension = {'typeCode': 'Wall width', 'unitCode': 'Inch', 'measure': wall_width}
            dimensions.append(dimension)

        if A and A in necessary_values[family]:
            dimension = {'typeCode': 'A', 'unitCode': 'Inch', 'measure': A}
            dimensions.append(dimension)

        if B and B in necessary_values[family]:
            dimension = {'typeCode': 'B', 'unitCode': 'Inch', 'measure': B}
            dimensions.append(dimension)

        if C and not math.isnan(C):
            dimension = {'typeCode': 'C', 'unitCode': 'Inch', 'measure': C}
            dimensions.append(dimension)

        if D and not math.isnan(D):
            dimension = {'typeCode': 'D', 'unitCode': 'Inch', 'measure': D}
            dimensions.append(dimension)

        if kg_m and kg_m in necessary_values[family]:
            dimension = {'typeCode': 'Kg/m', 'unitCode': None, 'measure': kg_m}
            dimensions.append(dimension)

        if depth and kg_m in necessary_values[family]:
            dimension = {'typeCode': 'Depth', 'unitCode': None, 'measure': depth}
            dimensions.append(dimension)

        if diameter and diameter in necessary_values[family]:
            dimension = {'typeCode': 'External diameter', 'unitCode': 'Inch', 'measure': diameter}
            dimensions.append(dimension)
            dimension = {'typeCode': 'Nominal diameter', 'unitCode': 'Inch', 'measure': diameter}
            dimensions.append(dimension)

        if cedula:
            try:
                float(cedula) # si se puede convertir la dejamos en Depth
                dimension = {'typeCode': 'Depth', 'unitCode': None, 'measure': cedula}
            except ValueError:
                # si no se puede convertir es porque es XXS o X
                # Tengo que encontrarles una equivalencia
                equivalencia = self.map_width_to_master_code(cedula, skip_conversion=True)
                dimension = {'typeCode': 'Depth', 'unitCode': None, 'measure': equivalencia}
            dimensions.append(dimension)
            
        return dimensions
    
    def update_item(
        self,
        item_id:int,
        **kwargs
    ) -> Response:
        """
        This function allows the user to update an existing item in ichigo

        :param item_id: id of the item that wants to be updated
        :param kwargs: 
            | finish -> finishCode
            | name -> name
            | grade -> gradeCode
            | classification -> classificationCode
            | presentation -> presentationCode
            | material -> materialCode
            | category -> categoryCode
            | weight -> weight
        """
        variables = {'id':item_id}
        variables.update(kwargs)
        json_data = {
            'query': 'mutation UpdateItemCatalog($updateItemCatalogInput: UpdateItemCatalogInput!) {\n  updateItemCatalog(updateItemCatalogInput: $updateItemCatalogInput) {\n    id\n  }\n}',
            'variables': {
                'updateItemCatalogInput': variables,
            },
            'operationName': 'UpdateItemCatalog',
        }
        
        response = requests.post(self.__endpoint, headers=self.__headers, json=json_data)
        return response