import re
import unicodedata
from PIL import Image
from bs4 import BeautifulSoup
from pyzbar.pyzbar import decode
from playwright.async_api import async_playwright
from pdf2image import convert_from_bytes, convert_from_path
from streamlit.runtime.uploaded_file_manager import UploadedFile

class InformationExtractor:
    '''
    Esta clase permite extraer la información de una constancia de situación
    fiscal a partir del pdf que el usuario suba, usando funcionalidades de Playwright
    de manera asíncrona.
    '''
    def __init__(self) -> None:
        self.information = None

    async def process_pdf(self, pdf: UploadedFile, from_path: bool = False) -> dict:
        img = self.__convert_pdf_to_image(pdf, from_path=from_path)
        url = self.__get_url_from_csf(img)
        html = await self.__get_html(url)
        information = self.__extract_information(html)
        self.information = information

    def __eliminar_acentos(self, texto):
        texto_normalizado = unicodedata.normalize('NFKD', texto)
        texto_sin_acentos = ''.join(c for c in texto_normalizado if not unicodedata.combining(c))
        return texto_sin_acentos

    def __clean_string(self, text: str) -> str:
        text = re.sub(r'[\n_()\-:]+', ' ', text)
        text = self.__eliminar_acentos(text)
        text = re.sub(r'\s+', '_', text.strip().lower())
        return text

    def __extract_information(self, html: str) -> dict:
        soup = BeautifulSoup(html, 'lxml')
        values = {}
        rfc = soup.find('li').text.split(':')[1].split(',')[0].strip()
        values['rfc'] = rfc
        tds = soup.select("tr[class='ui-widget-content'] td")[1:]
        previous_span = ''
        for td in tds:
            span = td.find('span')
            if span:
                previous_span = self.__clean_string(span.text)
            else:
                value = td.text.strip() if td.text.strip() != '' else None
                if value:
                    values[previous_span] = value
        return values

    def __convert_pdf_to_image(self, pdf: UploadedFile, from_path: bool = False) -> Image:
        if from_path:
            return convert_from_path(pdf, dpi=300)[0]
        if not pdf.name.endswith('pdf'):
            raise ValueError('File format not supported')
        bytes_data = pdf.getvalue()
        return convert_from_bytes(bytes_data, dpi=300)[0]

    def __get_url_from_csf(self, image: Image) -> str:
        return decode(image)[0].data.decode('ascii')

    async def __get_html(self, url: str) -> str:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            await page.goto(url, wait_until='domcontentloaded')
            content = await page.content()
            await browser.close()
            return content