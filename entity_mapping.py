# -*- coding: utf-8 -*-
"""
Created on Wed Aug 07 15:59:30 2019

@author: sliu439
"""

import pandas as pd
import jieba
from gensim import corpora,models,similarities


class entity_mapping():
    
    def __init__(self,mapping_filepath=r"F:\MTGE\automation\exch_etl\mapping.xlsx"):
        
        self.mapping=pd.read_excel(mapping_filepath)
        self.colname="chinese_name"
    
    def cut_chinese_name_from_mapping(self):
        
        all_word_list=[]
        chinese_name= self.mapping[self.colname]
        for ele in chinese_name:
            all_word_list.append([word for word in jieba.cut(ele)])
        return all_word_list
        
    def tfidf(self,key):
        
        word_key= [word for word in jieba.cut(key)]
        all_word_list= self.cut_chinese_name_from_mapping()
        dictionary = corpora.Dictionary(all_word_list)
        corpus = [dictionary.doc2bow(doc) for doc in all_word_list]
        doc_test_vec = dictionary.doc2bow(word_key)
        tfidf = models.TfidfModel(corpus)
        index = similarities.SparseMatrixSimilarity(tfidf[corpus], num_features=len(dictionary.keys()))
        sim = sorted(enumerate(index[tfidf[doc_test_vec]]), key=lambda item: -item[1])
        return sim[0][0] if len(sim)>0 else None
    
    def get_eng_name(self,key):
        
        i=self.tfidf(key)
        if i:
            return '{0}/{1}'.format(str(self.mapping.iloc[i]["english_name"]),str(self.mapping.iloc[i]["code"]))
        else:
            return ""
    
    
#entity_mapping_1 = entity_mapping(r'F:\MTGE\automation\exch_etl\mapping.xlsx') 
#res=entity_mapping_1.get_eng_name(u"上海银行股份有限公司")    
#print res 