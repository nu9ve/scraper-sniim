import datetime
from distutils.command.build_scripts import first_line_re
import requests
from bs4 import BeautifulSoup
# from sniim.db.mongo import Mongoclient
from clint.textui import puts, colored, indent
import time
import os
import pandas as pd
from unidecode import unidecode

import shutil


def read_csv_files():
  saved = []
  # for _, _, files in os.walk('./data/'):
  #   for file in files:
  #     saved.append(file.replace('sniim_producto_', '').replace('.csv', ''))
  return saved

last_year = 2024
start_year = 2018
# start_year = 2020

def find_data_table_index(tables):
  index = -1

  for i, t in enumerate(tables):
    # print('class="Datos"' in str(t))
    # print(i)
    if 'class="Datos"' in str(t):
      index = i
      break
    # time.sleep(10)


  return index

def get_requests_range(start_date, end_date):
  dates = []
  for y in range(start_date, end_date):
    for m in range(1,13):
      mstr = str(m) if m > 9 else '0'+str(m)
      date_1 = dict(mes=mstr,anio=str(y))
      date_1["del"] = '01'
      date_1["al"] = '15'
      date_2 = dict(mes=mstr,anio=str(y))
      date_2["del"] = '16'
      date_2["al"] = '31'
      dates.append(date_1)
      dates.append(date_2)
  return dates


def get_category_payload(category, date):
  payload = date
  payload["RegPag"] = '1000'
  if category == "Bovino":
    payload["origen"] = '0'
    payload["destino"] = '0'
  elif category == "Porcino":
    payload["destino"] = '0'
    payload["prod"] = '0'
  elif category == "Huevo":
    payload["sem"] = '0'
  elif category == "PolloEntero":
    payload["origen"] = '0'
    payload["destino"] = '0'
    payload["prod"] = '0'
  elif category == "PolloPieza":
    payload["origen"] = '0'
    payload["destino"] = '0'
    payload["prod"] = '0'
    

  return payload

class ScrapperMarketPecuario:
  total_records = 0
  inserted_records = 0
  current_product = 'None'
  first_print = True
  category = "None"

  base_url = 'http://www.economia-sniim.gob.mx/SNIIM-Pecuarios-Nacionales/'
  init_urls = [
    # ['Bovino', 'SelCor.asp?var=Bov', "Cor.asp?Var=Bov"],
    # ["Porcino", 'e_SelObr.asp?var=Por', 'Obr.asp?Var=Por'],
    # ["Huevo", 'e_SelHue.asp?', 'Hue.asp'],
    # ["PolloEntero", 'e_SelEnt.asp?', 'Ent.asp'],
    ["PolloPieza", 'e_SelPza.asp?', 'Pza.asp'],
  ]

  # http://www.economia-sniim.gob.mx/Nuevo/Home.aspx?opcion=Consultas/MercadosNacionales/PreciosDeMercado/Agricolas/ConsultaVolumenesIngreso.aspx?SubOpcion=10|0

  saved_products = read_csv_files()
  saved_products_clean = [unidecode(x).replace('/','') for x in saved_products ]

  all_products = []
  

  def __init__(self, *args, **kwargs):
    self.is_historic = kwargs.get('is_historic', True)
    # self.mongo = Mongoclient(db_collection='agricultura')
    self.df = pd.DataFrame()


  def read_category(self, category, url, url_form):
    print(self.base_url + url)
    category_page = requests.get(self.base_url + url)
    category_page = BeautifulSoup(category_page.content, features="html.parser")
    print(category)
    ct = "".join([ x[0] for x in category.lower().split(' ')])
    print(ct)

    self.df = pd.DataFrame()

    for date in get_requests_range(start_year, last_year):
      payload = get_category_payload(category, date)
      if not self.gather_prices(payload, url_form):
        continue
      time.sleep(4)

  def scraping(self):
    self.total_records = 0
    self.inserted_records = 0

    for category, url, url_form in self.init_urls:
      self.category = category
      self.read_category(category, url, url_form)

  def gather_prices(self, payload, url_form):
    with indent(4):
      puts(colored.blue("Peticion: {}".format(str(payload))))
    
    response = requests.get(self.base_url + url_form, params=payload)
    # print(self.base_url + url_form)
    # print(payload)

    if response.status_code != 200:
      with indent(4):
        puts(colored.red("Error en la peticion HTTP: {}".format(str(response.text))))
      return False

    missing_pages = True
    while missing_pages:
      missing_pages = False
        
      product_prices = BeautifulSoup(response.content.decode('latin-1'), features="html.parser")

      # pagination = product_prices.select_one('span#lblPaginacion').getText().split(' ')[-1]
      # print(product_prices)
      try:
        table_prices = product_prices.find_all('table')
        table_index = find_data_table_index(table_prices)
        if len(table_prices) > 1:
          table_prices = table_prices[table_index]
          # print(table_prices)
      except Exception as error:
        with indent(4):
          puts(colored.red("Error en el parseo: {}".format(str(error))))
        return False

      if self.category == "Bovino":
        fields = ('fecha', 'origen', 'corte', 'precio_min', 'precio_max')
      elif self.category == "Porcino":
        fields = ('fecha', 'corte', 'precio_kg')
      elif self.category == "Huevo":
        fields = ('fecha', 'producto', 'presentacion', 'precio_frecuente', 'precio_min', 'precio_max')
      elif self.category == "PolloEntero":
        fields = ('fecha', 'producto', 'marca', 'origen', 'peso_pie_kg', 'peso_canal_kg', 'precio_kg')
      elif self.category == "PolloPieza":
        fields = ('fecha', 
                  'pechuga_precio_min_kg', 'pechuga_precio_max_kg', 'pechuga_precio_frec_kg', 
                  'muslo_precio_min_kg', 'muslo_precio_max_kg', 'muslo_precio_frec_kg', 
                  'retazo_precio_min_kg', 'retazo_precio_max_kg', 'retazo_precio_frec_kg', 
                  'viscera_precio_min_kg', 'viscera_precio_max_kg', 'viscera_precio_frec_kg')
      else:
        print("THW")
        print("THW")
        time.sleep(30)
      counter_row = 0

      observation_destination = "None"
      for observation in table_prices.find_all('tr'):
        if 'class="encabTAB"' in str(observation):
          continue

        if 'class="encabDES"' in str(observation):
          observation_destination = observation.find("td").getText()
          # print(observation_destination)
          continue
        
        if 'JavaScript' in str(observation) or 'Insurgentes Sur' in str(observation):
          continue

        # print(observation)
        # time.sleep(10)
        # if counter_row > 1:
        row = {}
        counter_field = 0
        if self.first_print:
          # print(observation.find_all('td'))
          self.first_print = False
        for metric in observation.find_all('td'):
          # print('metric', metric)
          row[fields[counter_field]] = metric.getText()
          counter_field += 1

        # with indent(4):
        #   puts(colored.yellow("Insertando: {}".format(str(row))))

        # print(row)
        row['category'] = self.category
        row['destino'] = observation_destination

        if self.category == "Bovino":
          row['name'] = row["corte"].strip() + " de Res"
          row['corte'] = row['corte'].strip()
        if self.category == "Porcino":
          row['name'] = row["corte"].strip() + " de Puerco"
          row['corte'] = row['corte'].strip()
        if self.category == "Huevo":
          row['name'] = row["producto"]
        if self.category == "PolloEntero":
          row['name'] = row["producto"]
        # if self.category == "Bovino":
        #   row['name'] = row["corte"] + " de Res"
        # if self.category == "Bovino":
        #   row['name'] = row["corte"] + " de Res"
        df2 = pd.DataFrame(row, index=[0])
        self.df = pd.concat([self.df, df2])


        self.total_records += 1
        counter_row += 1
        # print("somerows", row)
        if self.total_records % 1000 == 0:
          self.df.to_csv(f'sniim_producto_{self.category.lower()}.csv', index=False)
      
      # product_prices.select_one() # wanted to click the ibtnSiguiente to get next pages


    return True


if __name__ == '__main__':
  agricola = ScrapperMarketPecuario()
  agricola.scraping()

  # vacas = ScrapperMarketLiveStock()
  # vacas.scraping()