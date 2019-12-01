# -*- coding: utf-8 -*-
import pandas as pd
import re
from datetime import  datetime
from entity_mapping import EntityMapping

SAIL_FIELDS_MAPPING = {"Deal Full Name": "DealName",
                       "DealID": "DealID",
                       "EXCHANGE NAME": "Exch",
                       "Type": "DealType",
                       "Asset Manager": "AssetM",
                       "Lead Manager": "LM",
                       "Originator": "Originator",
                       "Class Name": "ClsName",
                       "Chinese ID": "ChineseID",
                       "CHINA ID ": "ChinaID",
                       "Original Balance": "TrancheOriBal",
                       "Issue Amount": "DealAmount",
                       "SETTLEMENT DATE": "SetDt",
                       "FIRST PAYMENT DATE": "FirPayDt",
                       "MATURITY DATE": "FinalMty",
                       "Expected MATURITY DATE": "ExpMty",
                       "ORIGINAL COUPON": "Cpn",
                       "INTEREST FREQUENCY": "IntFreq",
                       "PRINCIPAL FREQUENCY": "PrinFreq",
                       "LISTING DATE": "LstDt",
                       "Rating": "Rating",
                       "Rating Agency": "RtAgency"
                       }
TRANCH_COLS = ["ClsName", "ChineseID", "ChinaID",
               "TrancheOriBal", "SetDt",
               "FirPayDt", "ExpMty", "FinalMty",
               "Cpn", "IntFreq", "ClsDes"
               ]
entity_mapper= EntityMapping()


def scan(pattern, filename):
    if re.match(pattern, filename):
        return True
    return False


def load_sail_template(filepath):
    if filepath.split(".")[-1].upper() == "XLSX":
        return pd.read_excel(filepath)
    else:
        print "file format wrong"


def to_index(mapping, headers, rtype):
    index_mapping = {}
    if rtype == "str":
        for key in mapping.keys():
            if key in headers:
                index_mapping[mapping[key]] = headers.index(key)
    elif rtype == "list":
        for key in mapping.keys():
            if key in headers:
                if mapping[key] in index_mapping.keys():
                    index_mapping[mapping[key]].append(headers.index(key))
                else:
                    index_mapping[mapping[key]] = [headers.index(key)]
    return index_mapping


def groupby_dataframe(df, column="DealID"):
    # for key, value in df.groupby(column):
    #    yield value.reset_index(drop=True)
    return (value.reset_index(drop=True) for key, value in df.groupby(column))


def convert_freq(string):
    res = ""
    if string == u"季付":
        res = "Q"
    elif string == u"半年付":
        res = "SA"
    elif string == u"年付" or u"到期偿还":
        res = "A"
    elif string == u"月付":
        res = "M"
    return res


def convert_waterfallfreq(string):
    res = ""
    if string == "Q":
        res = 4
    elif string == "A":
        res = 1
    elif string == "M":
        res = 12
    elif string == "SA":
        res = 2
    return res


def return_clsdes(FirPayDt, FinalMty, ClsName):
    res = ""
    if ClsName.upper() == "SUB":
        res = "SUB"
    elif FirPayDt == FinalMty:
        res = "HB"
    else:
        res = "PT,SEQ"
    return res


def convert_exch(string):
    res = ""
    if string == u"上海证券交易所":
        res = "Shanghai"
    elif string == u"深圳证券交易所":
        res = "Shenzhen"
    elif string == u"银行间债券市场":
        res = "China Interbank"
    return res


def map_entity(string):
    return entity_mapper.get_eng_name(string)


def normalize(df, index_mapping):
    df = df.fillna("")
    deal = {}
    tranches = []
    for index, row in df.iterrows():
        tranche = {}
        if index == 0:
            for key in index_mapping.keys():
                if key not in TRANCH_COLS:
                    if key == "Exch":
                        deal[key] = convert_exch(row.iloc[index_mapping[key]])
                    elif key == "AssetM" or key == "LM":
                        deal[key] = [map_entity(ele) for ele in row.iloc[index_mapping[key]].split(r",")]
                    elif key in ["LstDt"]:
                        try:
                            deal[key] = datetime.strptime(row.iloc[index_mapping[key]], '%m/%d/%Y')
                        except ValueError:
                            deal[key] = ""
                            print "please check data format of {0}:{1}".format(key, row.iloc[index_mapping[key]])
                    else:
                        deal[key] = row.iloc[index_mapping[key]]
        for key in TRANCH_COLS:
            if index_mapping.has_key(key):
                if key == "IntFreq":
                    tranche[key] = convert_freq(row.iloc[index_mapping[key]])
                elif key in ["FirPayDt", "FinalMty", "LstDt", "ExpMty", "SetDt"]:
                    try:
                        tranche[key] = datetime.strptime(row.iloc[index_mapping[key]], '%m/%d/%Y')
                    except ValueError:
                        tranche[key] = ""
                        print "please check data format of {0}:{1}".format(key, row.iloc[index_mapping[key]])
                else:
                    tranche[key] = row.iloc[index_mapping[key]]
        if tranche.has_key("FirPayDt") and tranche.has_key("FinalMty") and tranche.has_key("ClsName"):
            tranche["ClsDes"] = return_clsdes(tranche["FirPayDt"], tranche["FinalMty"], tranche["ClsName"])
        tranches.append(tranche)
    deal["tranches"] = tranches
    return deal
