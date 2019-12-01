# -*- coding: utf-8 -*-
"""
Created on Thu Aug 01 08:42:28 2019

@author: sliu439
"""


import bcos
import const
import csv_writer
import msg
import preprocess


msger = msg.Msg('sliu439@bloomberg.net', 'apmort@bloomberg.net')
mtge_bcos_info=bcos.BCOSINFO(const.BCOS_BUCKET_NAME, const.BCOS_ACCOUT, const.BCOS_SECKET_KEY)
mtge_bcos=bcos.Bcos(mtge_bcos_info, 'windows', 1)


def main():
    res = mtge_bcos.monitor(const.BCOS_KEY, const.BCOS_FREQ)
    for i in res:
        if preprocess.scan(".*_-Template-.*.xlsx", i):
            try:
                flag, filepath = mtge_bcos.download(i, const.BCOS_DOWNLOAD_PATH + str(i).split(r"/")[-1])
                if flag:
                    df = preprocess.load_sail_template(filepath)
                    index_mapping_sail = preprocess.to_index(preprocess.SAIL_FIELDS_MAPPING, df.columns.tolist(), "str")
                    for df in preprocess.groupby_dataframe(df):
                        deal = preprocess.normalize(df, index_mapping_sail)
                        excel_writer = csv_writer.Writer(const.EXCEL_TEMPLATE_PATH,const.EXCEL_OUTPUT_PATH + \
                                                         deal["DealName"] + ".XLSX", deal)
                        excel_writer.write2clsinfo("Class Information")
                        excel_writer.write2collainfo("Collateral Information")
                        excel_writer.write2dealinfo("Deal Information")
                        excel_writer.write2relatedPar("Related Parties")
                        msger.send("SAIL Daily File - {0}".format(deal["DealName"].encode('utf-8')),
                                   'Please find attached deal',
                                    const.EXCEL_OUTPUT_PATH + deal[
                                       "DealName"] + ".XLSX",
                                   deal["DealName"].encode('utf-8') + "_issuance_new_deal_template.XLSX")
            except Exception as e:
                print e.message


if __name__ == "__main__":

    main()