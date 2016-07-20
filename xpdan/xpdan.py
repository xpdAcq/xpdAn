import os
import yaml
import numpy as np
import pandas as pd

#from databroker.databroker import get_table
#from databroker.databroker import DataBroker as db

from xpdacq.new_xpdacq.glbl import glbl

########### helper function #########

def _get_current_saf():
    bt_list= [f for f in os.listdir(glbl.yaml_dir) if f.startswith('bt_')]
    if len(bt_list) != 1:
        raise RuntimeError("There are more than one beamtime objects in"
                           "{}. Please either gives specific saf or"
                           .format(glbl.yaml_dir))
    with open(os.path.join(glbl.yaml_dir, bt_list[0]), 'r') as f:
        bt = yaml.load(f)
        saf = bt['bt_safN']
        return saf


def _complete_shape(array):
    if len(np.shape(array)) == 1:
        output_array = np.expand_dims(array, 0)
    else:
        output_array = array
    return output_array


def _update_default_dict(default_dict, **kwargs):
    output_dict = dict(default_dict)
    output_dict.update(kwargs)
    return output_dict


######################################

class XpdAn:
    """ calss that holds databroker headers/events and summarize the
    metadata
    """

    _default_dict = {'group':'XPD'}

    def __init__(self, *, saf_num=None, **kwargs):
        self.header_md_fields = ['sa_name', 'timestamp']
        self.event_md_fiedls = None
        self._search_dict = None
        self._current_table = None
        self._current_search = None
        if saf_num is None:
            # current saf
            saf_num = _get_current_saf()
            self._search_dict = _update_default_dict(self._default_dict,
                                                     bt_safN=saf_num)

    def __getitem__(self, ind):
        return self._current_search[ind]


    @property
    def search_dict(self):
        """ propery that returns current search key-value pair """
        return self._search_dict


    @property
    def md_fields(self):
        """ fileds of metadata that will be summarized """
        return self._md_fields


    @md_fields.setter
    def md_fields(self, new_fields):
        self._md_fields = new_fields


    @property
    def current_search(self):
        """ property holds a list of header(s) from most recent search """
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


    def list(self, *, search_dict=None, **kwargs):
        """ method that lists headers """
        if search_dict is None:
            search_dict = self._search_dict
        search_dict.update(kwargs)
        self._set_current_search(db(**search_dict))
        self._tabel_gen(self._current_search,
                        col_name=self.header_md_fields)

    def _table_gen(self, header, ind_name=None, col_name=None):
        """ self-maintained thin layer of pd.dataFrame, and print

        mature function in databroker.core.get_table
        """
        col_len = len(col_name)

        # prepare md array
        header_md = []
        for h in headers:
            _md_info = []
            for field in header_md_fields:
                _md_info.append(h['start'][field])
            header_md.append(_md_info)
        md_array = np.asarray(header_md)
        md_array.resize((len(header_md)/col_len, col_len))
        # complete_shape
        pd_data = _complete_shape(md_array)
        data_dim = np.shape(data)
        col_dim = np.shape(col_name)
        # check if need transpose
        if data_dim[0] == col_dim[0] and data_dim != col_dim:
            data = np.transpose(data)
        pd_table = pd.DataFrame(data, ind_name, col_name)
        print(pd_table)
        self._set_current_table(pd_table)


