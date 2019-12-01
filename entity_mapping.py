# -*- coding: utf-8 -*-
"""
Module used to for find string similarities (TF-IDF)
"""
import pandas as pd

import jieba
from gensim import corpora, models, similarities

MAPPINGFILE="mapping.xlsx"
COL="chinese_name"

class EntityMapping():

    def __init__(self):

        self.mapping = pd.read_excel(MAPPINGFILE)
        self.colname = COL

    def __cut_chinese_name_from_mapping(self):

        all_word_list = []
        chinese_name = self.mapping[self.colname]
        for ele in chinese_name:
            all_word_list.append([word for word in jieba.cut(ele)])
        return all_word_list

    def __tf_idf(self, key):

        word_key = [word for word in jieba.cut(key)]
        all_word_list = self.__cut_chinese_name_from_mapping()
        dictionary = corpora.Dictionary(all_word_list)
        corpus = [dictionary.doc2bow(doc) for doc in all_word_list]
        doc_test_vec = dictionary.doc2bow(word_key)
        tfidf = models.TfidfModel(corpus)
        index = similarities.SparseMatrixSimilarity(tfidf[corpus], num_features=len(dictionary.keys()))
        sim = sorted(enumerate(index[tfidf[doc_test_vec]]), key=lambda item: -item[1])
        return sim[0][0] if len(sim) > 0 else None

    def get_eng_name(self, key):
        """take chinese name and return english name and code"""
        i = self.__tf_idf(key)
        if i:
            return '{0}/{1}'.format(str(self.mapping.iloc[i]["english_name"]), str(self.mapping.iloc[i]["code"]))
        else:
            return ""
