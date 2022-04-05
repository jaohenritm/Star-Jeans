# Imports
import os
import pandas as pd
import requests
import numpy as np
import re
import sqlite3
import logging

from sqlalchemy import create_engine
from datetime   import datetime
from bs4        import BeautifulSoup

# Data Collection
def data_collection( url, headers):

    # request to url
    page = requests.get(url, headers=headers)

    # beautiful soup object
    soup = BeautifulSoup(page.text, 'html.parser')

    # ========================= Product Data ========================= #
    products = soup.find('ul', class_= 'products-listing small')
    product_list = products.find_all('article',class_= 'hm-product-item')

    # product id
    product_id = [p.get('data-articlecode') for p in product_list]

    # product category
    product_category = [p.get('data-category') for p in product_list]

    data = pd.DataFrame([product_id, product_category]).T
    data.columns = ['product_id', 'product_category']

    return data

# Data Collection by Product
def data_collection_by_product(data, headers):

    # unique columns for all products
    aux = []

    cols = ['Art. No.', 'Composition']
    df_pattern = pd.DataFrame(columns=cols)

    # empty dataframe
    df_compositions = pd.DataFrame()

    for i in range(len(data)):
        # API Requests
        url = 'https://www2.hm.com/en_us/productpage.' + data.loc[i, 'product_id'] + '.html'
        logger.debug('Product: %s', url)

        page = requests.get(url, headers=headers)

        # Beautiful Soup object
        soup = BeautifulSoup(page.text, 'html.parser')

        ################### df_color ###################
        product_list = soup.find_all('a', class_='filter-option miniature active') + soup.find_all('a',
                                                                                                   class_='filter-option miniature')
        color_name = [p.get('data-color') for p in product_list]

        # product id
        product_id = [p.get('data-articlecode') for p in product_list]

        df_color = pd.DataFrame([product_id, color_name]).T
        df_color.columns = ['product_id', 'color_name']

        for j in range(len(df_color)):
            # API Requests
            url = 'https://www2.hm.com/en_us/productpage.' + df_color.loc[j, 'product_id'] + '.html'
            logger.debug('Color: %s', url)

            page = requests.get(url, headers=headers)

            # Beautiful Soup object
            soup = BeautifulSoup(page.text, 'html.parser')

            # product name
            product_name = soup.find_all('h1', class_='primary product-item-headline')
            product_name = product_name[0].get_text()

            # product price
            product_price = soup.find_all('div', class_='primary-row product-item-price')
            product_price = re.findall(r'\d+\.?\d+', product_price[0].get_text())[0]

            ################### composition ################

            product_composition_list = soup.find_all('div', class_='details-attributes-list-item')
            product_composition = [list(filter(None, p.get_text().split('\n'))) for p in product_composition_list]

            if product_composition != []:
                # reframe
                df_composition = pd.DataFrame(product_composition).T
                df_composition.columns = df_composition.iloc[0]
                df_composition = df_composition[df_composition['Composition'].notnull()]

                df_composition = df_composition[['Composition', 'Art. No.']]

                # delete first row and NA
                df_composition = df_composition.iloc[1:].fillna(method='ffill')
                df_composition = df_composition.iloc[:2]

                # remove pocket lining, shell and lining
                df_composition['Composition'] = df_composition['Composition'].str.replace('Pocket lining: ', '', regex=True)
                df_composition['Composition'] = df_composition['Composition'].str.replace('Shell: ', '', regex=True)
                df_composition['Composition'] = df_composition['Composition'].str.replace('Lining: ', '', regex=True)

                # renaming and organizing columns
                df_composition = df_composition[['Art. No.', 'Composition']]
                df_composition.columns = ['product_id', 'composition']

                df_composition['product_name'] = product_name
                df_composition['product_price'] = product_price

                # keep new columns if t shows up
                aux = aux + df_composition.columns.tolist()

                # merge
                df_composition = pd.merge(df_composition, df_color, how='left', on='product_id')

                # all products
                df_compositions = pd.concat([df_compositions, df_composition], axis=0)
            else:
                None

    df_compositions['style_id'] = df_compositions['product_id'].apply(lambda x: x[:-3])
    df_compositions['color_id'] = df_compositions['product_id'].apply(lambda x: x[-3:])

    # scrapy datetime
    df_compositions['scrapy-datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return df_compositions

# Data Cleaning
def data_cleaning(data_product):
    # product_id
    df_data = data_product.dropna(subset=['product_id'])

    # product_name
    df_data['product_name'] = df_data['product_name'].str.replace('\n', '')
    df_data['product_name'] = df_data['product_name'].str.replace('\t', '')
    df_data['product_name'] = df_data['product_name'].str.replace('  ', '')
    df_data['product_name'] = df_data['product_name'].str.replace(' ', '_').str.lower()

    # product price
    df_data['product_price'] = df_data['product_price'].astype(float)

    # product_color
    df_data['color_name'] = df_data['color_name'].str.replace(' ', '_').str.lower()

    # renaming columns
    df_data = df_data.rename(columns = {'Composition':'composition','scrapy-datetime':'scrapy_datetime'})

    # break composition by comma
    df1 = df_data['composition'].str.split(',', expand=True).reset_index(drop=True)

    # ================== cotton | polyester | elastomultiester | spandex | modal |
    df_ref = pd.DataFrame(index=np.arange(len(df_data)), columns=['cotton', 'polyester', 'spandex', 'modal',
                                                                 'elastomultiester'])

    # ==================  cotton
    df_cotton_0 = df1.loc[df1[0].str.contains('Cotton', na=True), 0]
    df_cotton_0.name = 'cotton'

    df_cotton_1 = df1.loc[df1[1].str.contains('Cotton', na=True), 1]
    df_cotton_1.name = 'cotton'

    # combine
    df_cotton = df_cotton_0.combine_first(df_cotton_1)

    df_ref = pd.concat([df_ref, df_cotton], axis=1)
    df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated(keep='last')]

    # ================== polyester
    df_polyester_0 = df1.loc[df1[0].str.contains('Polyester', na=True), 0]
    df_polyester_0.name = 'polyester'

    df_polyester_1 = df1.loc[df1[1].str.contains('Polyester', na=True), 1]
    df_polyester_1.name = 'polyester'

    # combine
    df_polyester = df_polyester_0.combine_first(df_polyester_1)

    df_ref = pd.concat([df_ref, df_polyester], axis=1)
    df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated(keep='last')]

    # ================== spandex
    df_spandex_1 = df1.loc[df1[1].str.contains('Spandex', na=True), 1]
    df_spandex_1.name = 'spandex'

    df_spandex_2 = df1.loc[df1[2].str.contains('Spandex', na=True), 2]
    df_spandex_2.name = 'spandex'

    df_spandex_3 = df1.loc[df1[3].str.contains('Spandex', na=True), 3]
    df_spandex_3.name = 'spandex'


    # combine
    df_spandex_c2 = df_spandex_1.combine_first(df_spandex_2)
    df_spandex = df_spandex_c2.combine_first(df_spandex_3)

    df_ref = pd.concat([df_ref, df_spandex], axis=1)
    df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated(keep='last')]

    # ================== elastomultiester
    df_elastomultiester = df1.loc[df1[1].str.contains('Elastomultiester', na=True), 1]
    df_elastomultiester.name = 'elastomultiester'

    # combine
    df_ref = pd.concat([df_ref, df_elastomultiester], axis=1)
    df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated(keep='last')]

    # ================== modal
    df_modal = df1.loc[df1[2].str.contains('Modal', na=True), 2]
    df_modal.name = 'modal'

    # combine
    df_ref = pd.concat([df_ref, df_modal], axis=1)
    df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated(keep='last')]

    # join of combine with product_id
    df_aux = pd.concat([df_data['product_id'].reset_index(drop=True), df_ref], axis=1)

    # breaking composition
    df_aux['cotton'] = df_aux['cotton'].apply(lambda x: int(re.search('Cotton (\d*)', x).group(1)) / 100 if (pd.notnull(x)) and ('Cotton' in x) else 0)
    df_aux['spandex'] = df_aux['spandex'].apply(lambda x: int(re.search('Spandex (\d*)', x).group(1)) / 100 if pd.notnull(x) and ('Spandex' in x) else 0)
    df_aux['polyester'] = df_aux['polyester'].apply(lambda x: int(re.search('Polyester (\d*)', x).group(1)) / 100 if pd.notnull(x) and ('Polyester' in x) else 0)
    df_aux['modal'] = df_aux['modal'].apply(lambda x: int(re.search('Modal (\d*)', x).group(1)) / 100 if pd.notnull(x) and ('Modal' in x) else 0)
    df_aux['elastomultiester'] = df_aux['elastomultiester'].apply(lambda x: int(re.search('Elastomultiester (\d*)', x).group(1)) / 100 if pd.notnull(x) and ('Elastomultiester' in x) else 0)


    # final join
    df_aux = df_aux.groupby('product_id').max().reset_index()
    df_data = pd.merge(df_data, df_aux, on='product_id', how='left')

    # drop columns
    df_data = df_data.drop(columns = 'composition', axis=1)

    # drop duplicates
    df_data = df_data.drop_duplicates().reset_index(drop=True)

    return df_data

# Data Insert
def data_insert(df_data):
    data_insert = df_data[[
        'product_id',
        'style_id',
        'color_id',
        'product_name',
        'color_name',
        'product_price',
        'cotton',
        'polyester',
        'spandex',
        'modal',
        'elastomultiester',
        'scrapy_datetime'
    ]]

    # create database connection
    conn = create_engine('sqlite:///database_hm.sqlite', echo=False)

    # data insert
    data_insert.to_sql('vitrine', con=conn, if_exists='append', index=False)

    return None

if __name__ == '__main__':
    # logging
    path = '/home/joaohenritm/repos/Star-Jeans/'

    if not os.path.exists(path + 'Logs'):
        os.makedirs(path + 'Logs')

    logging.basicConfig(
        filename = path + 'Logs/webscraping_hm.log',
        level = logging.DEBUG,
        format = '%(asctime)s - %(levelname)s - %(name)s - %(message)s -',
        datefmt = '%Y-%m-%d %H:%M:%S'
    )

    logger = logging.getLogger('webscraping_hm')

    # Parameters and constants
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36'}

    # url
    url = 'https://www2.hm.com/en_us/men/products/jeans.html?page-size=108'

    # Data Collection
    data = data_collection(url, headers)
    logger.info('data collection done')

    # Data Collection by Product
    data_product = data_collection_by_product(data, headers)
    logger.info('data collection by product done')

    # Data Cleaning
    data_product_cleaned = data_cleaning(data_product)
    logger.info('data product cleaned done')

    # Data Insertion
    data_insert(data_product_cleaned)
    logger.info('data insertion done')