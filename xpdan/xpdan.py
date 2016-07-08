from databroker.databroker import DataBroker as db

class xpdAn:
    """ class that pull out headers belongs to given beamtime
    """

    _default_dict = {'group':'XPD'}

    def __init__(self, saf_num, is_prun=True, is_setup=False):
        print("====Analysis calss has been instantiated====")
        #self._saf_num = saf_num
        self.saf_num = self.update_saf(saf_num)

    def update_saf(self, saf_num):
        """ update saf """
        self.saf_num = saf_num
        self.headers = self._all_headers()


    def _all_headers(self):
        """ methods that pull out all headers"""
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


    #@property
    #def saf_num(self):
    #    return self._saf_num

    #@saf_num.setter
    #def saf_num(self, val):
    #    self._saf_num = str(val)
    #    self.headers = self._all_headers()

    def list(self, *, group_index=None, **kwargs):
        """ method that lists/filter headers """
        _default_key = 'sa_name' # default grouping
        if not kwargs:
            group_list = self._filter(_default_key)
        else:
            if len(kwargs) != 1:
                print("WARNING: we only allows one search key now")
            elif len(kwargs) == 1:
                key = list(kwargs.keys()).pop()
                val = list(kwargs.values()).pop()
                group_list = self._filter(key, val)
                self._search_rv = group_list
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
                    unique_list = list(set(full_list))
            group_list = []
            for el in unique_list:
                search_dict = self._default_dict.update({key:el})
                h_list = [h for h in self.headers if h.start[key] == el]
                #h_list = db(**search_dict)
                group_list.append(h_list)
            print("Required field: {} resulting in following grouping:\n"
                  .format(key))
            for ind, el in enumerate(group_list):
                print("{}  has  {}  headers under current"
                      "SAF number".format(unique_list[ind], len(el)))
            print("INFO: to subsample this list, please gives the key value"
                  " and do `an.list(key=value)` again")
        else:
            # give specific header list
            group_list = [h for h in self.headers if h.start.get(key) == value]
            print("INFO: search with '{} = {}' has pulled out {}"
                  "headers".format(len(group_list)))

        return group_list


