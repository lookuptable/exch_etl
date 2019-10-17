# -*- coding: utf-8 -*-
"""
Created on Fri Mar 09 18:06:34 2018

@author: sliu439
"""

import bqapi


class SecfBqapi(object):

    session = bqapi.Session()

    def find_securities(self, query, max_results=5,
                        yk_filter='YK_FILTER_NONE', language_override='LANG_OVERRIDE_NONE',
                        callback=None, error_callback=None):
        """Summary
        Args:
            session (TYPE): bqapi session
            query (string): query string for the target securites
            max_results (int, optional): specify the number of results returned
            yk_filter (str, optional): yellow key filter
            language_ooverride (str, optional): language override, e.g LANG_OVERRIDE_CHINESE_SIMP
            callback (None, optional): callback function
            error_callback (None, optional): error handler
        Returns:
            list: list of dictionary discribe the found securities
        """
        def format_results(results):
            return results[0]['results']

        request_data = {
            'query': query,
            'maxResults':  max_results,
            'yellowKeyFilter': yk_filter,
            'languageOverride': language_override
        }

        format = self.session.make_format(
            bqapi.no_format, format_args=None, request_type='pubbdsesvc')

        results = self.session.send_request(
            '//blp/instruments', 'instrumentListRequest', request_data,
            format,
            callback=callback, error_callback=error_callback)
        return format_results(results)

        #print format_results(results)