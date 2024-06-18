import re
import unicodedata
from PIL import Image
from bs4 import BeautifulSoup
from pyzbar.pyzbar import decode
from pdf2image import convert_from_bytes
from playwright.sync_api import sync_playwright
from streamlit.runtime.uploaded_file_manager import UploadedFile

class InformationExtractor:
    '''
    Esta clase sirve para poder extraer la información de una constancia de situación
    fiscal a partir del pdf que el usuario suba. Por ahora está pensado exclusivamente
    para usarse dentro de un streamlit, pero en realidad es muy sencillo cambiarlo
    para que funcione si recibe la imagen desde cualquier otro lugar.
    '''
    def __init__(self) -> None:
        '''
        :param pdf: es opcional, pero si se pasa, se procesa automáticamente el pdf
        '''
        self.information = None

    def process_pdf(self, pdf:UploadedFile) -> dict:
        '''
        Esta funcón implementa todos los pasos necesarios para pasar de un pdf que el usuario
        haya subido hasta un diccionario que contenga la información extraida.

        :param pdf: UploadedFile como los de Streamlit

        :return: diccionario con la información extraida de la constancia
        '''
        img = self.__convert_pdf_to_image(pdf)

        # Convertimos la imagen a un url
        url = self.__get_url_from_csf(img)

        # Sacamos el html
        html = self.__get_html(url)

        # Extraemos la información pedida
        information = self.__extract_information(html)

        self.information = information


    def __eliminar_acentos(self, texto):
        # Normalizar el texto a la forma NFKD para separar los caracteres de sus marcas diacríticas
        texto_normalizado = unicodedata.normalize('NFKD', texto)
        # Filtrar para quedarse solo con los caracteres que no son combinaciones diacríticas
        texto_sin_acentos = ''.join(c for c in texto_normalizado if not unicodedata.combining(c))
        return texto_sin_acentos

    def __clean_string(self, text:str) -> str:
        text = text.replace('\n', ' ')
        text = text.replace('_', ' ')
        text = text.strip()
        text = text.lower()
        text = self.__eliminar_acentos(text)
        text = text.replace('(', '').replace(')', '').replace('-', ' ').replace(':', '')
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'_+', '_', text)
        text = text.replace(' ', '_')
        return text

    def __extract_information(self, html:str) -> dict:
        '''
        Esta función se encarga de extraer la información relevante del html que se le haya pasado.

        :param html: string con un html
        :return: diccionario con los campos encontrados
        '''
        soup = BeautifulSoup(html, 'lxml')

        values = {}

        rfc = soup.find('li').text.split(':')[1].split(',')[0].strip()
        values['rfc'] = rfc

        tds = soup.select("tr[class='ui-widget-content'] td")[1:]

        next_break = False
        for td in tds:
            span = td.find('span')
            if span:
                if span.text == 'AL:': next_break = True
                previous_span = self.__clean_string(span.text)
            else:
                value = td.text.strip() if td.text.strip() != '' else None
                if previous_span == '': continue
                values[previous_span] = value
                if next_break: break

        return values


    def __convert_pdf_to_image(self, pdf:UploadedFile) -> Image:
        '''
        Esta función convierte el pdf que el usuario subió a una imagen.

        :param pdf: UploadedFile de streamlit

        :return: image
        '''
        if not pdf.name.endswith('pdf'):
            raise('File format not supported')

        bytes_data = pdf.getvalue()
        img = convert_from_bytes(bytes_data, dpi=300)[0] # solo tomamos la primera página
        return img

    def __get_url_from_csf(self, image:Image) -> str:
        '''
        Esta función recibe una imagen conteniendo un código qr y regresa el link que haya encontrado

        :param image: imagen con algún qr

        :return: string con el link que haya encontrado
        '''
        return decode(image)[0].data.decode('ascii')

    def __get_html(self, url:str) -> str:
        '''
        Esta función recibe un url y regresa el html correspondiente
        utilizando playwright. Es útil cuando un simple request no es
        suficiente para sacar la información.

        :param url: url to download
        :return: string containing an html
        '''
        # Convertimos el url a html usando playwright
        # No olvidar instalar playwright en la consola
        # playwright install
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)  # Cambia a True si no necesitas ver el navegador
            context = browser.new_context()
            page = context.new_page()

            page.goto(url, wait_until='domcontentloaded')
            content = page.content()

        return content