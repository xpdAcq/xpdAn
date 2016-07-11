import pandas as pd
import numpy as np
from databroker.databroker import DataBroker as db

class xpdAn:
    """ class that pull out headers belongs to given beamtime
    """

    _default_dict = {'group':'XPD'}

    def __init__(self, saf_num, is_prun=True, is_setup=False):
        print("====Analysis calss has been instantiated====")
        self.saf_num = self.update_saf(saf_num)
        self._current_search = None
        self._current_table = None
        #FIXME - add hook to is_prun, is_setup filter

    def update_saf(self, saf_num):
        """ update saf and pull out all headers belong to this saf """
        self.saf_num = saf_num
        self.all_headers = self._all_headers()


    def _all_headers(self):
        """ methods that pull out all headers """
        self._default_dict.update(dict(bt_safN=self.saf_num))
        headers = db(**self._default_dict)
        self._default_dict.popitem()
        if len(headers) == 0:
            print("WARNING: there's no headers under this SAF"
                  "number={}, please make sure you have a valid SAF"
                  "number".format(self.saf_num))
            print("INFO: to update saf_number, please do"
                  "'an.update_saf(<saf_num>)' to update SAF"
                  "number associated with this analysis class" )
        print("INFO: there are {} headers associated with this"
              "SAF".format(len(headers)))
        return headers

    @property
    def current_search(self):
        return self._current_search

    @current_search.setter
    def current_search(self, header):
        print("INFO: `current_search` attribute has been updated")
        self._current_search = header

    @property
    def current_table(self):
        return self._current_table

    @current_table.setter
    def current_table(self, table):
        print("INFO: `current_table` attribute has been updated")
        self._current_table = table

    def list(self, *, group_index=None, **kwargs):
        """ method that lists/filter headers """
        _default_key = 'sa_name' # default grouping
        if not kwargs:
            group_list = self._filter(_default_key)
            self._current_search = group_list
        else:
            if len(kwargs) != 1:
                print("WARNING: we only allows one search key now")
            elif len(kwargs) == 1:
                key = list(kwargs.keys()).pop()
                val = list(kwargs.values()).pop()
                group_list = self._filter(key, val)
                self._current_search = group_list
            else:
                print('WARNING: oppps')
        #FIXME add group_index method


    def _filter(self, key, value=None):
        """ assume a flat md_dict so everything is in 'start' """
        if value is None:
            # give a summary
            #full_list = [h.start[key] for h in self.headers]
            full_list = []
            for h in self.all_headers:
                if key in h.start:
                    full_list.append(h.start[key])
                else:
                    full_list.append('missing sample name')
                unique_list = list(set(full_list))
            group_list = []
            for el in unique_list:
                h_list = [h for h in self.all_headers if h.start[key] == el]
                group_list.append(h_list)
            print("Required field: {} resulting in following grouping:"
                  .format(key))
            self._table_gen([group_list,list(map(len, group_list))],
                            row_name=unique_list,
                            col_name=['header', '# of headers'])
            print("INFO: to subsample this list, please gives the key value"
                  " and do `an.list(key=value)` again")
        else:
            # give specific header list
            group_list = [h for h in self.all_headers if h.start.get(key) == value]
            print("INFO: search with '{} = {}' has pulled out {}"
                  "headers".format(key, value, len(group_list)))
            self._table_gen([group_list,list(map(len, group_list))],
                            row_name=value,
                            col_name=['header', '# of headers'])
        return group_list

    def _header_and_len(self, header):
        """ samll function to return [header, len(header)] """
        return [header,list(map(len, header))]

    def _table_gen(self, data, row_name=None, col_name=None):
        """ thin layer of pd.dataFrame to include print """
        # normalize dim
        data_dim = np.shape(data)
        col_dim = np.shape(col_name)
        row_dim = np.shape(row_name)
        if data_dim[0] == col_dim[0] and data_dim[1] == row_dim[0]:
            data = np.transpose(data)
        pd_table = pd.DataFrame(data, row_name, col_name)
        print(pd_table)
        self._current_table = pd_table
        return pd_table
