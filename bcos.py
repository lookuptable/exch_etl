# -*- coding: utf-8 -*-
"""

This module is used to connect with bcos-bucket,which is web share dirve

Attributes
----------
BCOSINFO : namedtuple
    variable for bcos default config
BCOS_URL : list
    const for different bcos urls:dev,prod
PROXIES: dict
    const for different proxies: http,https
"""
from collections import namedtuple
from io import BytesIO

import requests

from lxml import etree
import time

BCOSINFO = namedtuple('BCOSINFO', ['bucket_name', 'account', 'secret_key'])
BCOS_URL = ['https://bcos.dev.blpprofessional.com:8443/v1',
            'https://bcos.prod.blpprofessional.com/v1']
PROXIES = {
    'http': 'bproxy.tdmz1.bloomberg.com:80',
    'https': 'bproxy.tdmz1.bloomberg.com:80'
}


class Environment(object):
    Dev = 0
    Prod = 1


class Bcos(object):
    def __init__(self, bcos_info, machine_env, env):
        self.bcos_info = bcos_info
        self.machine_env = machine_env
        self.env = env
        self.headers = {'x-bbg-bcos-account': self.bcos_info.account,
                        'x-bbg-bcos-secret-key': self.bcos_info.secret_key}

    def __get(self, url):
        """
        private function to download from bcos

        Parameters
        ----------
        url : str
             weburl for download
        Returns:
        ----------
        res : BytesIO
            file in Bytes format
        """
        try:
            print("Downloding from " + url)
            header = {
                'x-bbg-bcos-account': self.bcos_info.account,
                'x-bbg-bcos-secret-key': self.bcos_info.secret_key}
            proxies = {} if self.machine_env == "unix" else PROXIES
            response = requests.get(url, headers=header, proxies=proxies)
            if response.status_code == 200:
                return BytesIO(response.content)
        except Exception as ex:
            print("Failed to download: Error: " + str(ex.message))
            return None

    def download(self, bcos_key, output_filepath):
        """
         function to download file to local path

        Parameters
        ----------
        bcos_key : str
            keyword used to search for matching item on bcos
        output_filepath : str
            local filepath to save download file
        Returns:
        ----------
        res : bool
             download success or not
        output_filepath : str
             local filepath to save download file
        """
        url = '{0}/{1}/{2}'.format(BCOS_URL[self.env], self.bcos_info.bucket_name, bcos_key)
        stream = self.__get(url)
        if stream is not None:
            with open(output_filepath, 'wb') as f:
                f.write(stream.read())
                print 'Success download to' + output_filepath
            return True, output_filepath
        return False, ""

    def __search(self, bcos_key):
        """
        private function used to search for matching items on bcos

        Parameters
        ----------
        bcos_key : str
            keyword used to search for matching item on bcos
        Returns:
        ----------
        bcos_dict : dict
             key : matching file name
             value: matching file in BytesIO
        bcos_likst : list
             list of matching file names
        """
        params = {'prefix': bcos_key} if bcos_key is not None else {}
        is_truncated = True
        marker = None
        bcos_dict = {}
        bcos_list = []

        while is_truncated:
            if marker:
                params['marker'] = marker
            r = requests.get("{}/{}".format(BCOS_URL[self.env], self.bcos_info.bucket_name), headers=self.headers,
                             params=params, proxies=PROXIES)
            tree = etree.fromstring(r.text)
            if len(tree.xpath("*[name() = 'Contents']")) != 0:
                if tree[7].text == 'true':
                    marker = tree[3].text
                else:
                    is_truncated = False
                lxml_objs = tree.xpath("*[name() = 'Contents']")
                for obj in lxml_objs:
                    indivi_dict = {}
                    indivi_dict["LastModified"] = obj[1].text
                    indivi_dict["ETag"] = obj[2].text
                    indivi_dict["Size"] = obj[3].text
                    indivi_dict["StorageClass"] = obj[4].text
                    indivi_dict["Owner"] = obj[5].text
                    bcos_dict[obj[0].text] = indivi_dict
                    bcos_list.append(obj[0].text)
            else:
                print "no matching item found"
        return bcos_list, bcos_dict

    def monitor(self, bcos_key, freq):
        """
        function used to monitor bcos on frequent basis

        Parameters
        ----------
        bcos_key : str
            keyword used to search for mapping item on bcos
        freq : int
            frequency to search for bcos
        Returns:
        ----------
        content[0].text : generator
            new found matching filename
        """
        print "{} bucket monitoring initiated".format(self.bcos_info.bucket_name)
        marker = None
        params = {'prefix': bcos_key} if bcos_key is not None else {}
        bcos_list, bcos_dict = self.__search(bcos_key)

        if bcos_list:
            marker = bcos_list[-1]
            params["marker"] = marker
        else:
            params["marker"] = None
            print("Last snapshot is empty")
        try:
            while True:
                print "{} bucket is being monitored".format(self.bcos_info.bucket_name)
                r = requests.get("{}/{}".format(BCOS_URL[self.env], self.bcos_info.bucket_name), headers=self.headers,
                                 params=params, proxies=PROXIES)
                tree = etree.fromstring(r.text)
                contents = tree.xpath("*[name() = 'Contents']")
                if len(contents) == 1:
                    time.sleep(freq)
                    continue
                else:
                    for content in contents[1:]:
                        yield content[0].text
                    params['marker'] = contents[-1][0].text
                    time.sleep(freq)
        except KeyboardInterrupt:
            print "monitoring ended!"
