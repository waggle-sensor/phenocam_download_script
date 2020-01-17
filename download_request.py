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
from lxml import etree
from pathlib import Path


PHENOCAM_URL = "https://phenocam.sr.unh.edu"
REQUEST_URL = PHENOCAM_URL + "/webcam/network/download/"
LOGIN_URL= PHENOCAM_URL + "/webcam/accounts/login/"
TMPDIR = '/var/tmp'


file_permisson = 0o664
directory_permission = 0o775



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
    
    if verbose:
        print("file_prefix: ", file_prefix)
        print("mirror_files_directory: ", mirror_files_directory)

    absoulte_prefix = os.path.join(mirror_files_directory, file_prefix)
    if verbose:
        print("absoulte_prefix: ", absoulte_prefix)

    jpg_files_count = len(glob.glob(absoulte_prefix+"*.jpg"))

    # this does not recognize partial downloads
    return jpg_files_count 


def download(s, sitename, year, month, day, start_time, end_time):
    if day:
        start_date = "{}-{:02d}-{:02d}".format(year, month, day)
        end_date = "{}-{:02d}-{:02d}".format(year, month, day)
    else: 
        start_date = "{}-{:02d}-01".format(year, month)
        last_day = calendar.monthrange(year, month)[1]
        end_date = "{}-{:02d}-{:02d}".format(year, month, last_day)



    mirror_files_directory = os.path.join(mirrorDir, sitename, str(year), "{:02d}".format(month), "{:02d}".format(day))
        
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
    
    #"No files matching these criteria were found."
   
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

    form_data["start_time"] = start_time
    form_data["end_time"] = end_time
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
        sys.stderr.write('Extracting redirect url failed, continue\n')
        return
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



        

        if not os.path.isdir(mirrorDir):
            os.mkdir(mirrorDir)
            os.chmod(mirrorDir, directory_permission)

        siteDir = os.path.join(mirrorDir, sitename)
        if not os.path.isdir(siteDir):
            os.mkdir(siteDir)
            os.chmod(siteDir, directory_permission)

        yearDir = os.path.join(siteDir, str(year))
        if not os.path.isdir(yearDir):
            os.mkdir(yearDir)
            os.chmod(yearDir, directory_permission)


        monthDir = os.path.join(yearDir,  "{:02d}".format(month))
        if not os.path.isdir(monthDir):
            os.mkdir(monthDir)
            os.chmod(monthDir, directory_permission)

        targetDir = os.path.join(monthDir,  "{:02d}".format(day))
        if not os.path.isdir(targetDir):
            os.mkdir(targetDir)
            os.chmod(targetDir, directory_permission)
        


        #targetDir = os.path.join(mirrorDir, sitename, str(year), "{:02d}".format(month), "{:02d}".format(day))

        #Path(targetDir).mkdir(mode=directory_permission, parents=True, exist_ok=True)

        print("unzipping {} to {} ...".format(zip_file, targetDir))
        with zipfile.ZipFile(zip_file, 'r') as myzip:
            for member in myzip.namelist():
                filename = os.path.basename(member)
                # skip directories
                if not filename:
                    continue

                # copy file (taken from zipfile's extract)
                source = myzip.open(member)
                target_jpg = os.path.join(targetDir, filename)
                
                target = open(target_jpg, "wb")
                with source, target:
                    shutil.copyfileobj(source, target)
                os.chmod(target_jpg, file_permisson)

            #myzip.extractall(path=mirrorDir+'/')

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

    download_specs = []

    

    with open(config_file) as test:
        for line in test:
            #print(line)
            # remove comments
            line = line.split('#', 1)[0]
            
            line_array = line.split(',', 5)
            site_id = line_array[0].strip()
            if site_id == "":
                continue
            
            if len(line_array) < 5:
                print("Could not parse: ", line, line_array)
                sys.exit(1)

            years_str = line_array[1].strip()
            months_str = line_array[2].strip()
            start_time = line_array[3].strip()
            end_time = line_array[4].strip()

            if months_str == '*' or months_str == '':
                months = [ *range(1, 13) ] # all months
            else:
                months = months_str.split(';')
                months = [int(x) for x in months] # convert to ints

            if years_str == '*' or years_str == '':
                years_array = [2018, 2019, 2020]
            else:
                years_array = years_str.split(';')
                years_array = [int(x) for x in years_array] # convert to ints

            download_spec = {
                'site_id': site_id,
                'years': years_array,
                'months': months,
                'start_time': start_time,
                'end_time': end_time
            }

            if verbose:
                print(download_spec)
           # sys.exit(1)
            #print(site_id)



            download_specs.append(download_spec)
   

    if len(download_specs) == 0:
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


    #start_time = "00:00"
    #end_time = "23:59"

    #start_time = "10:00"
    #end_time = "16:29"


    # open a web session and login
    with requests.session() as s:


        login(s, username, password)

        for download_spec in download_specs:
            site_id = download_spec['site_id']
            years = download_spec['years']
            months = download_spec['months']
            start_time = download_spec['start_time']
            end_time = download_spec['end_time']

            r = requests.get('https://phenocam.sr.unh.edu/webcam/archive/sites/{}/?format=json'.format(site_id))
            if r.status_code != 200:
                sys.stderr.write("r.status_code:", r.status_code)
                sys.exit(1)

            monthly = json.loads(r.text)
            #print("monthly: ", monthly)
            
            monthly_file_counts = monthly["monthly_file_counts"]

            available_year_months={}
            for entry in monthly_file_counts:
                year = entry["year"]
                month = entry["month"]
                if not year in available_year_months:
                    available_year_months[year] = {}

                available_year_months[year][month]=1
            

            if verbose:
                print(available_year_months)

            for year in years:
                
                if not year in available_year_months:
                    continue

                #available_mo = available_months[year]

                for month in months:
                    if not month in available_year_months[year]:
                        continue

                    
                    if year < 2018:
                        print("skipping year ", year)
                        continue

                    
                    last_day = calendar.monthrange(year, month)[1]
                    for day in range(1, last_day):
                        print("download {} {}/{}/{}".format(site_id, year, month, day))
                        download(s, site_id, year, month, day, start_time , end_time)
                        

        
