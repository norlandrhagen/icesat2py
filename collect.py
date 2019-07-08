""" modifed from NSIDC DAAC ICESat-2 Customize and Access notebook from the UW icesat2 'hackweek' """
import requests
import getpass
import socket
import json
import zipfile
import io
import math
import os
import shutil
import pprint
import time
import geopandas as gpd
import matplotlib.pyplot as plt
import fiona
import h5py
import re
# To read KML files with geopandas, we will need to enable KML support in fiona (disabled by default)
fiona.drvsupport.supported_drivers['LIBKML'] = 'rw'
from shapely.geometry import Polygon, mapping
from shapely.geometry.polygon import orient
from statistics import mean
from requests.auth import HTTPBasicAuth

""" user inputs passed to eventuall subset function
required: earthdata credentials, product(atl06 etc)
startDate, endDate, spatial bounds (for now bbox)
opt: outputdir, list of variables, streaming y/n"""

"""current user inputs"""
uid = input('username: ')
pswd = input('password: ')
email = input('email: ')


product = 'ATL06'
short_name = 'ATL06'


# Input start date in yyyy-MM-dd format
start_date = '2018-09-01'
# Input start time in HH:mm:ss format
start_time = '00:00:00'
# Input end date in yyyy-MM-dd format
end_date = '2019-06-01'
# Input end time in HH:mm:ss format
end_time = '23:59:59'


# Input bounding box
# Input lower left longitude in decimal degrees
LL_lon = '-25.6'
# Input lower left latitude in decimal degrees
LL_lat = '63.2'
# Input upper right longitude in decimal degrees
UR_lon = '-11.5'
# Input upper right latitude in decimal degrees
UR_lat = '66.4'



variable_list = '/ancillary_data/atlas_sdp_gps_epoch,\
/gt1l/land_ice_segments/atl06_quality_summary,\
/gt1l/land_ice_segments/delta_time,\
/gt1l/land_ice_segments/h_li,\
/gt1l/land_ice_segments/h_li_sigma,\
/gt1l/land_ice_segments/latitude,\
/gt1l/land_ice_segments/longitude,\
/gt1l/land_ice_segments/segment_id,\
/gt1l/land_ice_segments/sigma_geo_h,\
/gt1r/land_ice_segments/atl06_quality_summary,\
/gt1r/land_ice_segments/delta_time,\
/gt1r/land_ice_segments/h_li,\
/gt1r/land_ice_segments/h_li_sigma,\
/gt1r/land_ice_segments/latitude,\
/gt1r/land_ice_segments/longitude,\
/gt1r/land_ice_segments/segment_id,\
/gt1r/land_ice_segments/sigma_geo_h,\
/gt2l/land_ice_segments/atl06_quality_summary,\
/gt2l/land_ice_segments/delta_time,\
/gt2l/land_ice_segments/h_li,\
/gt2l/land_ice_segments/h_li_sigma,\
/gt2l/land_ice_segments/latitude,\
/gt2l/land_ice_segments/longitude,\
/gt2l/land_ice_segments/segment_id,\
/gt2l/land_ice_segments/sigma_geo_h,\
/gt2r/land_ice_segments/atl06_quality_summary,\
/gt2r/land_ice_segments/delta_time,\
/gt2r/land_ice_segments/h_li,\
/gt2r/land_ice_segments/h_li_sigma,\
/gt2r/land_ice_segments/latitude,\
/gt2r/land_ice_segments/longitude,\
/gt2r/land_ice_segments/segment_id,\
/gt2r/land_ice_segments/sigma_geo_h,\
/gt3l/land_ice_segments/atl06_quality_summary,\
/gt3l/land_ice_segments/delta_time,\
/gt3l/land_ice_segments/h_li,\
/gt3l/land_ice_segments/h_li_sigma,\
/gt3l/land_ice_segments/latitude,\
/gt3l/land_ice_segments/longitude,\
/gt3l/land_ice_segments/segment_id,\
/gt3l/land_ice_segments/sigma_geo_h,\
/gt3r/land_ice_segments/atl06_quality_summary,\
/gt3r/land_ice_segments/delta_time,\
/gt3r/land_ice_segments/h_li,\
/gt3r/land_ice_segments/h_li_sigma,\
/gt3r/land_ice_segments/latitude,\
/gt3r/land_ice_segments/longitude,\
/gt3r/land_ice_segments/segment_id,\
/gt3r/land_ice_segments/sigma_geo_h,\
/orbit_info/cycle_number,\
/orbit_info/rgt,\
/orbit_info/orbit_number'






def collect(uid,email,pswd,product,start_date, start_time, end_date,end_time,LL_lon,LL_lat, UR_lon, UR_lat, variable_list=variable_list):
    temporal = start_date + 'T' + start_time + 'Z' + ',' + end_date + 'T' + end_time + 'Z'
    timevar = start_date + 'T' + start_time + ',' + end_date + 'T' + end_time
    # Commenting for tutorial since we will be walking through option 3 (spatial file input) together
    # Bounding Box spatial parameter in 'W,S,E,N' format
    bounding_box = LL_lon + ',' + LL_lat + ',' + UR_lon + ',' + UR_lat
    print(bounding_box)
    # Request token from Common Metadata Repository using Earthdata credentials
    token_api_url = 'https://cmr.earthdata.nasa.gov/legacy-services/rest/tokens'
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)

    data = {
        'token': {
            'username': uid,
            'password': pswd,
            'client_id': 'NSIDC_client_id',
            'user_ip_address': ip
        }
    }
    headers={'Accept': 'application/json'}
    response = requests.post(token_api_url, json=data, headers=headers)
    token = json.loads(response.content)['token']['id']
    params = {
        'short_name': product
    }
    cmr_collections_url = 'https://cmr.earthdata.nasa.gov/search/collections.json'
    response = requests.get(cmr_collections_url, params=params)
    results = json.loads(response.content)
    versions = [i['version_id'] for i in results['feed']['entry']]
    latest_version = max(versions)
    #Create CMR parameters used for granule search. Modify params depending on bounding_box or polygon input.
    params = {
    'short_name': short_name,
    'version': latest_version,
    'temporal': temporal,
    'page_size': 100,
    'page_num': 1,
    'bounding_box': bounding_box}
    granule_search_url = 'https://cmr.earthdata.nasa.gov/search/granules'
    granules = []
    while True:
        response = requests.get(granule_search_url, params=params, headers=headers)
        results = json.loads(response.content)

        if len(results['feed']['entry']) == 0:
            # Out of results, so break out of loop
            break

        # Collect results and increment page_num
        granules.extend(results['feed']['entry'])
        params['page_num'] += 1
    # Get number of granules over my area and time of interest
    print('Number of granules in spatiotemporal bounds: ', len(granules))
    granule_sizes = [float(granule['granule_size']) for granule in granules]
    # Average size of granules in MB
    print('Average size of granule in MB: ', mean(granule_sizes))
    # Total volume in MB
    print('Total size of granule(s) in MB: ', sum(granule_sizes))
    # Query service capability URL
    from xml.etree import ElementTree as ET
    capability_url = f'https://n5eil02u.ecs.nsidc.org/egi/capabilities/{short_name}.{latest_version}.xml'
    session = requests.session()
    s = session.get(capability_url)
    response = session.get(s.url,auth=(uid,pswd))
    root = ET.fromstring(response.content)
    subagent = [subset_agent.attrib for subset_agent in root.iter('SubsetAgent')]
    # variable subsetting
    variables = [SubsetVariable.attrib for SubsetVariable in root.iter('SubsetVariable')]
    variables_raw = [variables[i]['value'] for i in range(len(variables))]
    variables_join = [''.join(('/',v)) if v.startswith('/') == False else v for v in variables_raw]
    variable_vals = [v.replace(':', '/') for v in variables_join]
    # print(subagent)
    if len(subagent) < 1 :
        agent = 'NO'
    bbox = bounding_box
    base_url = 'https://n5eil02u.ecs.nsidc.org/egi/request'
    # Set number of granules requested per order, which we will initially set to 10.
    page_size = 10
    #Determine number of pages basd on page_size and total granules. Loop requests by this value
    page_num = math.ceil(len(granules)/page_size)
    #Set request mode.
    request_mode = 'async'
    # Determine how many individual orders we will request based on the number of granules requested
    print('Total number of orders requested: ', page_num)
    #Print API base URL + request parameters
    API_request = f'{base_url}?short_name={short_name}&version={latest_version}&temporal={temporal}&time={timevar}&Coverage={variable_list}&request_mode={request_mode}&page_size={page_size}&page_num={page_num}&token={token}&email={email}'
    # print(API_request)
    request_params = {
        'short_name': short_name,
        'version': latest_version,
        'temporal': temporal,
        'agent' : 'NO',
        'include_meta' : 'Y',
        'request_mode': request_mode,
        'page_size': page_size,
        'token': token,
        'email': email,
        }
    def mkoutputdir(outputdir = '/Outputs'):
        path = str(os.getcwd() + outputdir)
        if not os.path.exists(path):
            os.mkdir(path)
        return path
    path = mkoutputdir()

    def send_api_request():
        for i in range(page_num):
            page_val = i + 1
            request_params.update( {'page_num': page_val} )
        # For all requests other than spatial file upload, use get function
            request = session.get(base_url, params=request_params)
            # print('Request HTTP response: ', request.status_code)
        # Raise bad request: Loop will stop for bad response code.
            request.raise_for_status()
            # print('Order request URL: ', request.url)
            esir_root = ET.fromstring(request.content)
            # print('Order request response XML content: ', request.content)
        #Look up order ID
            orderlist = []
            for order in esir_root.findall("./order/"):
                orderlist.append(order.text)
            orderID = orderlist[0]
            # print('order ID: ', orderID)
        #Create status URL
            statusURL = base_url + '/' + orderID
            # print('status URL: ', statusURL)
        #Find order status
            request_response = session.get(statusURL)
            # print('HTTP response from order response URL: ', request_response.status_code)
        # Raise bad request: Loop will stop for bad response code.
            request_response.raise_for_status()
            request_root = ET.fromstring(request_response.content)
            statuslist = []
            for status in request_root.findall("./requestStatus/"):
                statuslist.append(status.text)
            status = statuslist[0]
            print('Data request ', page_val, ' is submitting...')
            print('Initial request status is ', status)
        #Continue loop while request is still processing
            while status == 'pending' or status == 'processing':
                print('Status is not complete. Trying again.')
                time.sleep(10)
                loop_response = session.get(statusURL)
        # Raise bad request: Loop will stop for bad response code.
                loop_response.raise_for_status()
                loop_root = ET.fromstring(loop_response.content)
        #find status
                statuslist = []
                for status in loop_root.findall("./requestStatus/"):
                    statuslist.append(status.text)
                status = statuslist[0]
                print('Retry request status is: ', status)
                if status == 'pending' or status == 'processing':
                    continue
        #Order can either complete, complete_with_errors, or fail:
        # Provide complete_with_errors error message:
            if status == 'complete_with_errors' or status == 'failed':
                messagelist = []
                for message in loop_root.findall("./processInfo/"):
                    messagelist.append(message.text)
                print('error messages:')
                pprint.pprint(messagelist)
        # Download zipped order if status is complete or complete_with_errors
            if status == 'complete' or status == 'complete_with_errors':
                downloadURL = 'https://n5eil02u.ecs.nsidc.org/esir/' + orderID + '.zip'
                print('Zip download URL: ', downloadURL)
                print('Beginning download of zipped output...')
                zip_response = session.get(downloadURL)
                # Raise bad request: Loop will stop for bad response code.
                zip_response.raise_for_status()
                with zipfile.ZipFile(io.BytesIO(zip_response.content)) as z:
                    z.extractall(path)
                print('Data request', page_val, 'is complete.')
            else: print('Request failed.')

    def send_api_request_streaming():
        print('Sending data request...')
        # Set page size to 1 to improve performance
        page_size = 1
        request_params.update( {'page_size': page_size})
        # No metadata to only return a single output
        request_params.update( {'include_meta': 'N'})
        #Determine number of pages basd on page_size and total granules. Loop requests by this value
        page_num = math.ceil(len(granules)/page_size)
        #Set request mode.
        request_params.update( {'request_mode': 'stream'})
        os.chdir(path)
        for i in range(page_num):
            page_val = i + 1
            print('Order: ', page_val)
            request_params.update( {'page_num': page_val})
            request = session.get(base_url, params=request_params)
            # print('HTTP response from order response URL: ', request.status_code)
            request.raise_for_status()
            d = request.headers['content-disposition']
            fname = re.findall('filename=(.+)', d)
            open(eval(fname[0]), 'wb').write(request.content)
            print('Data request', page_val, 'is complete.')

    def clean_outputdir():
        for root, dirs, files in os.walk(path, topdown=False):
            for file in files:
                try:
                    shutil.move(os.path.join(root, file), path)
                except OSError:
                    pass
        for root, dirs, files in os.walk(path):
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        sorted(os.listdir(path))


    contYN = input('Does this file size seem correct? [Y/n] ')
    if contYN == 'Y':
        send_api_request_streaming()
        clean_outputdir()
    else:
        print('Exiting... Please pick differant spatio temporal bounds and re-run')


collect(uid,email,pswd,product,start_date, start_time, end_date,end_time,LL_lon,LL_lat, UR_lon, UR_lat, variable_list)
