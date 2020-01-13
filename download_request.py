#!/usr/bin/env python3

import os
import sys
import time
import calendar
import shutil
import requests
import lxml.html
import logging
import re
import http.client as http_client
import argparse
import zipfile
import glob
import json


PHENOCAM_URL = "https://phenocam.sr.unh.edu"
REQUEST_URL = PHENOCAM_URL + "/webcam/network/download/"
LOGIN_URL= PHENOCAM_URL + "/webcam/accounts/login/"
TMPDIR = '/var/tmp'


def login(s, username, password):
    if verbose:
            print("GET request for login page")
    response = s.get(LOGIN_URL)
    if verbose:
        print("status: ", response.status_code)
    if response.status_code != 200:
        print("error, got status: {}".format(response.status_code))
        sys.exit(1)
        

    # grab the html from the login page
    login_html = lxml.html.fromstring(response.text)

    # get the hidden inputs
    hidden_inputs = login_html.xpath(r'//form//input[@type="hidden"]')

    # construct login form data from hidden inputs plus username
    # and password
    form_data = {x.attrib["name"]: x.attrib["value"] for x in hidden_inputs}
    # print("hidden form fields: ", form_data)
    form_data['username'] = username
    form_data['password'] = password
    form_data['submit'] = ""
    form_data['next'] = REQUEST_URL

    # update session referer
    s.headers.update({'referer': LOGIN_URL})

    # submit login form
    if verbose:
        print("POST request to login page")
    response = s.post(LOGIN_URL, data=form_data)
    if verbose:
        print("status: ", response.status_code)

    if response.status_code != 200:
        print("error, got status: {}".format(response.status_code))
        sys.exit(1)

    return


def glob_jpg_files(mirror_files_directory, sitename, year, month, day):
    #example: NEON.D19.CARI.DP1.20002_2019_12_20_230006.jpg
    
    file_prefix = "_".join([sitename, str(year), "{:02d}".format(month), "{:02d}".format(day)])
    

    absoulte_prefix = os.path.join(mirror_files_directory, file_prefix)
    if verbose:
        print("absoulte_prefix: ", absoulte_prefix)

    jpg_files_count = len(glob.glob(absoulte_prefix+"*.jpg"))

    # this does not recognize partial downloads
    return jpg_files_count 

def download(s, sitename, year, month, day):
    if day:
        start_date = "{}-{:02d}-{:02d}".format(year, month, day)
        end_date = "{}-{:02d}-{:02d}".format(year, month, day)
    else: 
        start_date = "{}-{:02d}-01".format(year, month)
        last_day = calendar.monthrange(year, month)[1]
        end_date = "{}-{:02d}-{:02d}".format(year, month, last_day)



    mirror_files_directory = os.path.join(mirrorDir,'phenocamdata', sitename, str(year), "{:02d}".format(month))
        
    if os.path.isdir(mirror_files_directory):
        jpg_files_count = glob_jpg_files(mirror_files_directory, sitename, year, month, day)
            

        # TODO: would be nice to know how many files are expected
        if jpg_files_count > 0:
            print("skipping download, files found in ", mirror_files_directory)
            return

    # get download form page
    if verbose:
        print("GET request to download page")
    response = s.get(REQUEST_URL)
    if verbose:
        print("status: ", response.status_code)
    if response.status_code != 200:
        print("error, got status: {}".format(response.status_code))
        sys.exit(1)

    # grab the html from the download page
    download_html = lxml.html.fromstring(response.text)

    # get the hidden inputs
    hidden_inputs = download_html.xpath(r'//form//input[@type="hidden"]')

    # construct login form data from hidden inputs plus username
    # and password
    form_data = {x.attrib["name"]: x.attrib["value"] for x in hidden_inputs}
    # prepare POST request for download submission
    form_data = {x.attrib["name"]: x.attrib["value"] for x in hidden_inputs}
    # print("hidden form fields: ", form_data)

    #
    form_data['submit'] = ""
    form_data['site'] = sitename

    
        

    form_data["start_date"] = start_date
    form_data["end_date"] = end_date

    form_data["start_time"] = "00:00"
    form_data["end_time"] = "23:59"
    form_data["ir_flag"] = ""

    # print("form data: ", form_data)

    # update session referer
    s.headers.update({'referer': REQUEST_URL})

    if verbose:
        print("POST to download page")
    r = s.post(REQUEST_URL, data=form_data)
    if verbose:
        print("status: {}".format(r.status_code))
    if r.status_code != 200:
        print("error, got status: {}".format(r.status_code))
        sys.exit(1)
    

    # parse page and get script which redirects 
    download_html = lxml.html.fromstring(r.text)
    scripts = download_html.xpath(r'//script')
    if len(scripts) < 4:
        if debug:
            print(r.text)
        sys.stderr.write('Error parsing response\n')
        sys.exit(1)
    redirect_script = scripts[3].text

    # extract redirect URL using regular expressions
    redirect_regex = re.compile('window.location.href = \'(.+)\'}')
    mo = redirect_regex.search(redirect_script)
    if mo == None:
        sys.stderr.write('Extracting redirect url failed\n')
        sys.exit(1)
    redirect_url = mo[1]
    redirect_url = PHENOCAM_URL + redirect_url
    # print('redirect URL: ', redirect_url)


    # get URL as a data stream
    if verbose:
        print("Get request to redirect_url "+ redirect_url)
    with s.get(redirect_url, stream=True,
                allow_redirects=False) as r:


    
        if day:
            outfileBase = '{}_{}_{}_{}.zip'.format(sitename, year, month, day)
        else:
            outfileBase = '{}_{}_{}.zip'.format(sitename, year, month)

        zip_file = os.path.join(TMPDIR, outfileBase)
 

        zip_file_part = zip_file + ".part"
        print("downloading file {}...".format(zip_file))
        with open(zip_file_part, 'wb') as f:
            # for chunk in r.iter_content(chunk_size=8192):
            #     if chunk:
            #         f.write(chunk)
            shutil.copyfileobj(r.raw, f)
        
        # final filename only when download complete
        os.rename(zip_file_part, zip_file)

        
        print("unzipping {} to {} ...".format(zip_file, mirrorDir))
        with zipfile.ZipFile(zip_file, 'r') as myzip:
            myzip.extractall(path=mirrorDir+'/')

        os.remove(zip_file)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Download PhenoCam Images for a Site, Year, Month")
    
    # options

    
    parser.add_argument("-c","--config",
                        help="config file with lists of data sets to mirror",
                        default = ""
                         )

    parser.add_argument("-v","--verbose",
                        help="increase output verbosity",
                        action="store_true",
                        default=False)

    parser.add_argument("-d","--debug",
                        help="log connections for debugging",
                        action="store_true",
                        default=False)



    # get env
    mirrorDir = os.getenv('PHENOCAM_MIRROR_DIR')

    # get args
    args = parser.parse_args()

    use_mirror = True        # no need to support other use-case

    config_file = args.config
    verbose = args.verbose
    debug = args.debug
  

    #print(config_file)

    site_ids = []

    with open(config_file) as test:
        for line in test:
            #print(line)
            # remove comments
            line = line.split('#', 1)[0]
            site_id = line.strip()
            if site_id == "":
                continue
            #print(site_id)
            site_ids.append(site_id)
   

    if len(site_ids) == 0:
        sys.stderr.write('No sites found in config')
        sys.exit(1)

    

    if use_mirror:
        if not mirrorDir:
            sys.stderr.write('please specifiy environment variable PHENOCAM_MIRROR_DIR')
            sys.exit(1)

        if not os.path.isdir(mirrorDir):
            sys.stderr.write('Mirror directory {} does not exist.\n'.format(mirrorDir))
            sys.exit(1)

    # set up connection logging if verbose
    if debug:
        http_client.HTTPConnection.debuglevel = 1
        logging.basicConfig()
        logging.getLogger().setLevel(logging.DEBUG)
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = True

    

    # get phenocam user and password from
    # environment variables
    username = os.getenv('PHENOCAM_USER')
    if username is None:
        sys.stderr.write('Set username in PHENOCAM_USER env var\n')
        sys.exit(1)
    password = os.getenv('PHENOCAM_PASSWD')
    if password is None:
        sys.stderr.write('Set password in PHENOCAM_PASSWD env var\n')
        sys.exit(1)


    

    # open a web session and login
    with requests.session() as s:


        login(s, username, password)

        for site_id in site_ids:

            r = requests.get('https://phenocam.sr.unh.edu/webcam/archive/sites/{}/?format=json'.format(site_id))
            if r.status_code != 200:
                sys.stderr.write("r.status_code:", r.status_code)
                sys.exit(1)

            monthly = json.loads(r.text)
            #print(monthly)
            monthly_file_counts = monthly["monthly_file_counts"]
    
            for entry in monthly_file_counts:
                year = entry["year"]
                month = entry["month"]

                last_day = calendar.monthrange(year, month)[1]
                for day in range(1, last_day):
                    print("download {} {}/{}/{}".format(site_id, year, month, day))
                    #download(s, site_id, year, month, day)

        
