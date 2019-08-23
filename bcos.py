import requests
from collections import namedtuple
from io import BytesIO
from lxml import etree
import time
import pytz
from dateutil import parser
import re

BcosInfo = namedtuple('BcosInfo', ['bucket_name', 'account', 'secret_key'])

class Environment(object):
    Dev = 0
    Prod = 1


BCOS_URL = ['https://bcos.dev.blpprofessional.com:8443/v1',
            'https://bcos.prod.blpprofessional.com/v1']
PROXIES = {
    'http': 'bproxy.tdmz1.bloomberg.com:80',
    'https': 'bproxy.tdmz1.bloomberg.com:80'
}

a
def get(url, bcos_info, machine_env):
    
    try:
        print("Downloding from " + url)
        header = {
            'x-bbg-bcos-account': bcos_info.account,
            'x-bbg-bcos-secret-key': bcos_info.secret_key}

        proxies = {} if machine_env == "unix" else PROXIES     
        response = requests.get(url, headers=header, proxies=proxies)
        if response.status_code == 200:
            return BytesIO(response.content)

    except Exception as ex:
        print("Failed to download: Error: " + str(ex.message))
        return None
        
def scan(pattern,filename):
    
    if re.match(pattern,filename):
        return True
    return False
    
def download(bcos_key, bcos_info, output_file_path, env, machine_env):
    
    url = '{0}/{1}/{2}'.format(BCOS_URL[env], bcos_info.bucket_name, bcos_key)
    stream = get(url, bcos_info, machine_env)
    
    if stream is not None:            
                with open('{0}/{1}'.format(output_file_path,bcos_key),'wb') as f:
                    f.write(stream.read())                                
                    print 'Success download to {0}/{1}'.format(output_file_path,bcos_key)
                return True, "{0}/{1}".format(output_file_path,bcos_key)
    return False    
    
def search(bcos_key,bcos_info,env):
    
    params={'prefix':bcos_key} if bcos_key is not None else {}
    headers = {'x-bbg-bcos-account': bcos_info.account,
            'x-bbg-bcos-secret-key': bcos_info.secret_key}
    is_truncated= True
    marker=None  
    bcos_dict={}
    bcos_list=[]
    
    
    while is_truncated:
        if marker:
            params['marker']=marker      
        r=requests.get("{}/{}".format(BCOS_URL[env],bcos_info.bucket_name), headers=headers,params=params,proxies=PROXIES)
        tree=etree.fromstring(r.text)
        print r.url
        if len(tree.xpath("*[name() = 'Contents']"))!=0:                  
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
    return bcos_list,bcos_dict

def searchbyDate(bcos_key,bcos_info,env,startDt,endDt,timezone="Asia/Shanghai"):
    
    ''' type of startDt, endDt: string 
        format of startDt, endDt: YYYY-MM-DD, YYYYMMDD
        rtype: list 
    '''
    res =[]
    startDt=parser.parse(startDt)
    endDt=parser.parse(endDt)
    local_tz= pytz.timezone(timezone)  
    bcos_list,bcos_dict = search(bcos_key,bcos_info,env)
    
    for key,value in bcos_dict.iteritems():
        utc_dt = parser.parse(value["LastModified"])
        local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz).replace(tzinfo=None) 
        if local_dt >= startDt and local_dt <= endDt :      
            res.append(key)
    return res   
    
def monitor(bcos_key,bcos_info,freq,env,machine_env):
    
    print "{} bucket monitoring initiated".format(bcos_info.bucket_name)
    
    marker = None
    params={'prefix':bcos_key} if bcos_key is not None else {}
    headers = {'x-bbg-bcos-account': bcos_info.account,
            'x-bbg-bcos-secret-key': bcos_info.secret_key}
    bcos_list,bcos_dict= search(bcos_key,bcos_info,env)

    if bcos_list:
        marker = bcos_list[-1]
        params["marker"] = marker
    else:
        params["marker"] = None
        print("Last snapshot is empty")
    try:    
        while True:
            print "{} bucket is being monitored".format(bcos_info.bucket_name)
            r = requests.get("{}/{}".format(BCOS_URL[env],bcos_info.bucket_name), headers=headers,params=params,proxies=PROXIES)
            tree = etree.fromstring(r.text)
            contents = tree.xpath("*[name() = 'Contents']")
            if len(contents) == 1:
                time.sleep(freq)
                continue
            else:
                for content in contents[1:]:
                    yield content[0].text #filename
                params['marker'] = contents[-1][0].text
                time.sleep(freq)
    except KeyboardInterrupt:
        print "monitoring ended!"
            
    
if __name__ == "__main__":
    bcosinfo=BcosInfo("gd-mtge","gd-mtge-bcos-account","1153f598e3e026a8e710d384dbd7483b0d3724d42c2bd937ccb83bb7f469ff85")
    res=monitor("CN_NEWS_AUTOMATION",bcosinfo,900,1,'windows')
    for i in res:
        i=i.split("//")[-1]
        print i
        if scan(".*_-Template-.*.xlsx",i):
            flag,filepath=download(i,bcosinfo,r"F:\MTGE\automation\exch_etl\\",1,'windows')
            print filepath


