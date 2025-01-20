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
# start_year = 1999
start_year = 2020

class ScrapperMarketAgriculture:
  total_records = 0
  inserted_records = 0
  current_product = 'None'
  first_print = True

  base_url = 'http://www.economia-sniim.gob.mx/NUEVO/Consultas/MercadosNacionales/PreciosDeMercado/Agricolas'
  base_url = 'http://www.economia-sniim.gob.mx/Nuevo/Home.aspx?opcion=Consultas/MercadosNacionales/PreciosDeMercado/Agricolas'
  init_urls = [
    # ['Frutas y Hortalizas', '/ConsultaFrutasYHortalizas.aspx', '/ResultadosConsultaFechaFrutasYHortalizas.aspx'],
    # ['Flores', '/ConsultaFlores.aspx?SubOpcion=5', '/ResultadosConsultaFechaFlores.aspx'],
    # ['Granos', '/ConsultaGranos.aspx?SubOpcion=6', '/ResultadosConsultaFechaGranos.aspx'],
    # ['Aceites', '/ConsultaAceites.aspx?SubOpcion=8', '/ResultadosConsultaFechaAceites.aspx'],
    ['Volumenes', '/ConsultaVolumenesIngreso.aspx?SubOpcion=10', 'FAKE/ConsultaVolumenesIngreso.aspx?SubOpcion=10|0']
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
    category_page = requests.get(category_page.find("ifraHome")["src"])
    print(category)
    ct = "".join([ x[0] for x in category.lower().split(' ')])
    print(ct)
    print(category_page)
    print(category_page.select_one('select#ddlProducto'))
    products = [(product.getText(), product['value'], ) for product in category_page.select_one('select#ddlProducto').find_all('option')]
   
    for product in products:
      product_name, product_id = product
      if product_id == '-1':
        continue

      # ### DELETE
      # print(product_name)
      # cprod = str(product_name).lower().replace('-','').replace('  ','_').replace(' ','_').replace('/','')
      # csprod = f'sniim_producto_{cprod}.csv'
      # print(csprod)
      # shutil.copyfile(os.path.join('./data',csprod), os.path.join('./productos_sniim',ct,f'{cprod}.csv'))
      # # break
      # continue
      # ### DELETE
      
      if self.current_product != 'None' and not self.df.empty:
        csv_name = self.current_product.replace('/','')
        self.df.to_csv(f'sniim_producto_{csv_name}.csv', index=False)
      self.current_product = str(product_name).lower().replace('-','').replace('  ','_').replace(' ','_').replace('/','')
      if unidecode(self.current_product).replace('/','') in self.saved_products_clean:
        continue
      self.first_print = True
      self.df = pd.DataFrame()
      with indent(4):
        # puts(colored.magenta("Producto: {}".format(str(product_name))))
        puts(colored.magenta("Producto: {}".format(self.current_product)))

      product_name_row = [product_id, product_name, unidecode(self.current_product).replace('/',''), category]
      self.all_products.append(product_name_row)
      print(product_name_row)

      continue
    
      if self.is_historic:
        for year in range(start_year, last_year):
          payload = {
              'fechaInicio':'01/01/{0}'.format(str(year)),
              'fechaFinal':'01/01/{0}'.format(str(year + 1)),
              'ProductoId':product_id,
              'OrigenId':'-1',
              'Origen':'Todos',
              'DestinoId':'-1',
              'Destino':'Todos',
              'PreciosPorId':'2',
              'RegistrosPorPagina':'1000'
          }
          # Semana: 
          # 4
          # Mes: 
          # 2
          # Anio: 
          # 2022
          # ProductoId: 
          # 598
          # OrigenId: 
          # -1
          # Origen: 
          # Todos
          # DestinoId: 
          # -1
          # Destino: 
          # Todos
          # RegistrosPorPagina: 
          # 500
          if not self.gather_prices(payload, url_form):
            continue
      else:
        today = datetime.datetime.today()
        deleta = datetime.timedelta(days=-1)
        payload = {
                'fechaInicio':'{}'.format(today.strftime('%d/%m/%Y')),
                'fechaFinal':'{}'.format((today).strftime('%d/%m/%Y')),
                'ProductoId':product_id,
                'OrigenId':'-1',
                'Origen':'Todos',
                'DestinoId':'-1',
                'Destino':'Todos',
                'PreciosPorId':'2',
                'RegistrosPorPagina':'1000'
            }

        if not self.gather_prices(payload, url_form):
          continue
    all_products_df = pd.DataFrame(self.all_products)
    all_products_df.to_csv(f'../../data/sniim/sniim_products_simple.csv')
    return

  def scraping(self):
    self.total_records = 0
    self.inserted_records = 0

    for category, url, url_form in self.init_urls:
      self.read_category(category, url, url_form)
      # time.sleep(20)
      time.sleep(1)
      ### DELETE
      # break


  def gather_prices(self, payload, url_form):
    with indent(4):
      puts(colored.blue("Peticion: {}".format(str(payload))))
    
    response = requests.get(self.base_url + url_form, params=payload)
    print(self.base_url + url_form)
    print(payload)
    # time.sleep(30)
    time.sleep(3)
    if response.status_code != 200:
      with indent(4):
        puts(colored.red("Error en la peticion HTTP: {}".format(str(response.text))))
      return False

    missing_pages = True
    while missing_pages:
      missing_pages = False
        
      product_prices = BeautifulSoup(response.content, features="html.parser")

      # pagination = product_prices.select_one('span#lblPaginacion').getText().split(' ')[-1]

      try:
        table_prices = product_prices.select_one('table#tblResultados')
      except Exception as error:
        with indent(4):
          puts(colored.red("Error en el parseo: {}".format(str(error))))
        return False

      fields = ('fecha', 'presentacion', 'origen', 'destino', 'precio_min', 'precio_max', 'precio_frec', 'obs')
      counter_row = 0

      # print(table_prices)
      # print(table_prices.find_all('tr'))
      # print(len(table_prices.find_all('tr')))
      for observation in table_prices.find_all('tr'):
        if counter_row > 1:
          row = {}
          counter_field = 0
          if self.first_print:
            # print(observation.find_all('td'))
            self.first_print = False
          for metric in observation.find_all('td'):
            row[fields[counter_field]] = metric.getText()
            counter_field += 1

          # with indent(4):
          #   puts(colored.yellow("Insertando: {}".format(str(row))))

          # print(row)
          row['name'] = self.current_product
          df2 = pd.DataFrame(row, index=[0])
          self.df = pd.concat([self.df, df2])
          # if self.mongo.insert_one(row):
          #   self.inserted_records += 1
          #   print(f'inserted {str(row)}')
          #   # with indent(4):
          #   #     puts(colored.green("Insertado: {}".format(str(row))))
          # else:
          #   print('not inserted')
          #   # with indent(4):
          #   #     puts(colored.red("No Insertado: {}".format(str(row))))
        self.total_records += 1
        counter_row += 1
        print("somerows", row)
        if self.total_records % 1000 == 0:
          self.df.to_csv(f'sniim_producto_{self.current_product}.csv', index=False)
      
      # product_prices.select_one() # wanted to click the ibtnSiguiente to get next pages


    return True


if __name__ == '__main__':
  agricola = ScrapperMarketAgriculture()
  agricola.scraping()

  # vacas = ScrapperMarketLiveStock()
  # vacas.scraping()