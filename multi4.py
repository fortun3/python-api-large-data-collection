from concurrent.futures import ThreadPoolExecutor
import itertools
from time import sleep
import requests
import db
from datetime import datetime
import sys

sys.setrecursionlimit(10**8)

count = 0


item_details = []


# demo account
APP_ID = 'APP_ID'
SECRET_KEY = 'SECRET_KEY'
USER_ID = 'USER_ID'



def refresh_token():

    oldToken = db.get_access_token()
    url = 'https://api.mercadolibre.com/oauth/token'
    headers = {
        'accept': 'application/json',
        'content-type': 'application/x-www-form-urlencoded',
    }
    data = 'grant_type=refresh_token&client_id='+APP_ID + \
        '&client_secret='+SECRET_KEY+'&refresh_token='+oldToken[2]

    response = requests.post(url, headers=headers, data=data)
    data = response.json()
    db.update_access_token(data)
    return data['access_token']


def get_item_id_by_offset(session, offset):
    access_token = db.get_access_token()[1]
    url = 'https://api.mercadolibre.com/users/'+USER_ID+'/items/search?offset=' + \
        str(offset)
    headers = {
        'Authorization': 'Bearer ' + access_token
    }
    response = session.get(url, headers=headers)
    data = response.json()

    # if access key is expired, refresh it
    if("error" in data):
        access_token = refresh_token()
        get_item_id_by_offset(session, offset)
    else:
        values = []
        for id in data['results']:
            values.append((id,))
        db.insert_items_id(values)


def get_scroll_id(session):
    access_token = db.get_access_token()[1]
    url = 'https://api.mercadolibre.com/users/'+USER_ID+'/items/search?search_type=scan'
    headers = {
        'Authorization': 'Bearer ' + access_token
    }
    response = session.get(url, headers=headers)
    data = response.json()
    if("error" in data):
        refresh_token()
        get_scroll_id(session)
    return data['scroll_id']



def get_item_details(session, url, access_token):

    headers = {
        'Authorization': 'Bearer ' + access_token
    }
    response = session.get(url, headers=headers)
    data = response.json()
    global item_details
    for item in data:
        item_details.append(item)

    if("error" in data):
        access_token = refresh_token()
        get_item_details(session, url, access_token)


def save_items_details():
    save_data = map_data()
    db.save_details(save_data)
    global item_details
    item_details = []


def map_data():
    i = 0
    datas_dict = {}
    column_names = []
    global item_details
    for item in item_details:
        i += 1
        if(type(item) == dict):
            key_list = list(item.keys())
            datas_dict[i] = {}
            for key in key_list:
                # if item[key] is not iterable then column name = key
                type_check = type(item[key]) == dict
                if (type_check) and hasattr(item[key], '__iter__'):
                    for key2 in list(item[key].keys()):
                        type_check = type(item[key][key2]) == dict
                        if (type_check) and hasattr(item[key][key2], '__iter__'):
                            for key3 in list(item[key][key2].keys()):
                                type_check = type(item[key][key2][key3]) == dict
                                if (type_check) and hasattr(item[key][key2][key3], '__iter__'):
                                    for key4 in list(item[key][key2][key3].keys()):
                                        type_check = type(
                                            item[key][key2][key3][key4]) == dict
                                        if (type_check) and hasattr(item[key][key2][key3][key4], '__iter__'):
                                            for key5 in list(item[key][key2][key3][key4].keys()):
                                                type_check = type(
                                                    item[key][key2][key3][key4][key5]) == dict
                                                if (type_check) and hasattr(item[key][key2][key3][key4][key5], '__iter__'):
                                                    for key6 in list(item[key][key2][key3][key4][key5].keys()):
                                                        column_names.append(
                                                            key+'__'+key2+'__'+key3+'__'+key4+'__'+key5+'__'+key6)
                                                        datas_dict[i][key+'__'+key2+'__'+key3+'__'+key4+'__' +
                                                                    key5+'__'+key6] = item[key][key2][key3][key4][key5][key6]
                                                else:
                                                    column_names.append(
                                                        key+'__'+key2+'__'+key3+'__'+key4+'__'+key5)
                                                    datas_dict[i][key+'__'+key2+'__'+key3+'__'+key4 +
                                                                '__'+key5] = item[key][key2][key3][key4][key5]
                                        else:
                                            column_names.append(
                                                key+'__'+key2+'__'+key3+'__'+key4)
                                            datas_dict[i][key+'__'+key2+'__'+key3 +
                                                        '__'+key4] = item[key][key2][key3][key4]
                                else:
                                    column_names.append(key+'__'+key2+'__'+key3)
                                    datas_dict[i][key+'__'+key2+'__' +
                                                key3] = item[key][key2][key3]                                                                
                        else:
                            column_names.append(key+'__'+key2)
                            datas_dict[i][key+'__'+key2] = item[key][key2]

                else:
                    column_names.append(key)
                    datas_dict[i][key] = item[key]

        # seller sku seperating here....
        if 'attributes' in item['body']:
            for listItem in item['body']['attributes']:
                if(type(listItem)==dict and listItem['id']=='SELLER_SKU'):
                    for key3 in list(listItem.keys()):
                        if(key3 == 'values' and isinstance(listItem[key3], list)):
                            for listItem2 in listItem[key3]:
                                if(type(listItem2) == dict):
                                    for key4 in list(listItem2.keys()):
                                        if(key4 == 'id'):
                                            column_names.append('body'+'__'+'attributes'+'__'+'seller_sku'+'_'+key3+'_'+key4)
                                            datas_dict[i]['body'+'__'+'attributes'+'__'+'seller_sku'+'_'+key3+'_'+key4] = listItem2[key4]
                        else:
                            column_names.append('body'+'__'+'attributes'+'__'+'seller_sku'+'_'+key3)
                            datas_dict[i]['body'+'__'+'attributes'+'__'+'seller_sku'+'_'+key3] = listItem[key3]


    column_names = list(set(column_names))
    db.make_columns_table(column_names)
    return datas_dict


# main code
if __name__ == '__main__':
    db.create_table()
    access_token = refresh_token()

    with requests.Session() as session:
        offset = 0
        while offset <= 1000:
            get_item_id_by_offset(session, offset)
            offset += 50

        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        print("Current Time Before Getting IDs =", current_time)

        scroll_id = get_scroll_id(session)
        values = []

        #item ids via scroll id API calls......
        while True:
            apiUrl = 'https://api.mercadolibre.com/users/'+USER_ID+'/items/search?search_type=scan&scroll_id='+scroll_id
            headers = {'Authorization': 'Bearer ' + access_token}

            response = session.get(apiUrl, headers=headers)
            data = response.json()

            try:
                if(data['results'] == []):
                    break
            except KeyError:
                if('error' in data and data['status'] == 401):
                    access_token = refresh_token()
                print(data)
                sleep(0.5)
                continue
            else:
                for id in data['results']:
                    values.append((id,))

     
        print("IDs fetched saving in database...")
        db.insert_items_id(values)
        print("id addition done")
        values = None

        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        print("Current Time After Getting IDs =", current_time)

        items_id = db.get_items_id()
        print("items id fetched")
        out = list(itertools.chain(*items_id))
        print("items id flattened")
        items_id = None

        #item details API calls and saving
        access_token = refresh_token()
        urls = []
        loop = 0
        while (out != []):
            count = 0
            url = 'https://api.mercadolibre.com/items?ids='
            for item in out:
                count += 1
                url = url + str(item)+','
                out.remove(item)
                if(count == 20):
                    break
            # remove the last , from the url
            url = url[:-1]
            urls.append(url)

            # 100 urls in - make 100 thread API calls
            if(len(urls)==100 or out == []):
                session_count = len(urls)
                loop += 1

                print("urls appended")
                

                with ThreadPoolExecutor(max_workers=100) as executor:
                    executor.map(get_item_details, [session]*session_count, urls, [access_token]*session_count)
                    executor.shutdown(wait=True)

                print("item details fetched")
                save_items_details()
                print("Saved Items Details in database..",loop)
                urls = []

        print("Finished saving all details..")
