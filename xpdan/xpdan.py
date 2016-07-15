import numpy as np
import pandas as pd
from databroker.databroker import DataBroker as db

class xpdAn:
    """ class that pull out headers belongs to given beamtime
    """

    _default_dict = {'group':'XPD'}

    def __init__(self, saf_num, is_prun=True, is_setup=False):
        print("====  Analysis calss has been instantiated  ====")
        self._saf_num = saf_num
        self._current_search = None
        self._current_table = None
        self.all_headers = self._all_headers()
        #FIXME - add hook to is_prun, is_setup filter

    @property
    def saf_num(self):
        return self._saf_num

    def update_saf(self, saf_num):
        """ update saf and pull out all headers belong to this saf 
        similar to property setter but want user to be specific
        """

        self._saf_num = saf_num
        self.all_headers = self._all_headers()

    def _all_headers(self):
        """ methods that pull out all headers """
        self._default_dict.update(dict(bt_safN=self._saf_num))
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
        """ property that holds a list of header(s) from most recent search """
        return self._current_search

    def _set_current_search(self, headers, *, index=None):
        print("INFO: attribute of `current search` has been updated.")
        if index is None:
            self._current_search = headers
        else:
            self._current_search = self._current_search[index]

    @property
    def current_table(self):
        """ property that holds a pd.DataFrames from most recent search """
        return self._current_table


    def _set_current_table(self, table):
        print("INFO: `current_table` attribute has been updated")
        self._current_table = table


    def list(self, **kwargs):
        """ method that lists/filter headers """
        _default_key = 'sa_name' # default grouping
        if not kwargs:
            # default setting
            group_list = self._filter(_default_key)
            self._set_current_search(group_list)
            print("INFO: to subsample this list, please gives the key value"
                  " and do `an.list(key=value)` again")
        else:
            if len(kwargs) != 1:
                print("WARNING: we only allows one search key now")
                return
            elif len(kwargs) == 1:
                key = list(kwargs.keys()).pop()
                val = list(kwargs.values()).pop()
                group_list = self._filter(key, val)
                self._set_current_search(group_list)
            else:
                print('WARNING: oppps')


    def get_index(self, group_ind=None):
        """ method to slicing current_search """
        if index is None:
            print("INFO: dude, you don't give me index. Nothing happen")
        self._set_current_search(index=group_ind)



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
            print("INFO: Required field: `{}` resulting in following grouping:"
                  .format(key))
            self._table_gen([unique_list,list(map(len, group_list))],
                            col_name=[key, '# of headers'])
        else:
            # give specific key-val pair
            group_list = [h for h in self.all_headers if h.start.get(key) == value]
            print("INFO: search with '{} = {}' has pulled out {} headers"
                  .format(key, value, len(group_list)))
            self._table_gen([value, len(group_list)],
                            col_name=[key, '# of headers'])
        return group_list

    def _table_gen(self, data, ind_name=None, col_name=None):
        """ thin layer of pd.dataFrame to include print """
        # complete_shape
        data = _complete_shape(data)
        data_dim = np.shape(data)
        col_dim = np.shape(col_name)
        # check if need transpose
        if data_dim[0] == col_dim[0] and data_dim != col_dim:
            data = np.transpose(data)
        pd_table = pd.DataFrame(data, ind_name, col_name)
        print(pd_table)
        self._set_current_table(pd_table)
        return pd_table


    def integrate(self):
        print('use SrX to integrate all images in side this header')
        pass


    def plot(self):
        print('plot all images from headers under current_search')
        pass

def _complete_shape(array):
    if len(np.shape(array)) == 1:
        output_array = np.expand_dims(array, 0)
    else:
        output_array = array
    return output_array
