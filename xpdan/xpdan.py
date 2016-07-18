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
        self.md_fields = ['sa_name', 'timestamp']
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


    def _table_gen(self, data, ind_name=None, col_name=None):
        """ thin layer of pd.dataFrame, including print

        maybe better to use get_table
        """
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


