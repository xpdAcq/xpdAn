import pandas as pd
from databroker.databroker import DataBroker as db

class xpdAn:
    """ class that pull out headers belongs to given beamtime
    """

    _default_dict = {'group':'XPD'}

    def __init__(self, saf_num, current_search=None, is_prun=True, is_setup=False):
        print("====Analysis calss has been instantiated====")
        self.saf_num = self.update_saf(saf_num)
        self._current_search = current_search
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


    def _set_current_search(self, headers):
        print("INFO: attribute of `current search` has been updated.")
        self._current_search = headers


    def list(self, *, group_index=None, **kwargs):
        """ method that lists/filter headers """
        _default_key = 'sa_name' # default grouping
        if not kwargs:
            group_list = self._filter(_default_key)
            self._set_current_search(group_list)
        else:
            if len(kwargs) != 1:
                print("WARNING: we only allows one search key now")
            elif len(kwargs) == 1:
                key = list(kwargs.keys()).pop()
                val = list(kwargs.values()).pop()
                group_list = self._filter(key, val)
                self._set_current_search(group_list)
            else:
                print('WARNING: oppps')
        #FIXME add group_index method


    def _filter(self, key, value=None):
        """ assume a flat md_dict so everything is in 'start' """
        if value is None:
            # give a summary
            #full_list = [h.start[key] for h in self.headers]
            full_list = []
            for h in self.headers:
                if key in h.start:
                    full_list.append(h.start[key])
                else:
                    full_list.append('missing sample name')
                unique_list = list(set(full_list))
            group_list = []
            for el in unique_list:
                h_list = [h for h in self.headers if h.start[key] == el]
                group_list.append(h_list)
            print("Required field: {} resulting in following grouping:"
                  .format(key))
            pd_table =self.table_gen([group_list,list(map(len,group_list))],
                                     row_name=unique_list,
                                     col_name=[key, '# of headers'])
            print("INFO: to subsample this list, please gives the key value"
                  " and do `an.list(key=value)` again")
        else:
            # give specific header list
            group_list = [h for h in self.headers if h.start.get(key) == value]
            print("INFO: search with '{} = {}' has pulled out {}"
                  "headers".format(len(group_list)))
        return group_list


    def table_gen(self, data, row_name=None, col_name=None):
        """ thin layer of pd.dataFrame to include print """
        pd_table = pd.DataFrame(data, row_name, col_name)
        print(pd_table)
        return pd_table
