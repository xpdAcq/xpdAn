##############################################################################
#
# xpdan            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2016 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Christopher J. Wright
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################
import os

from xpdan.data_reduction import (integrate_and_save, integrate_and_save_last,
                                  save_tiff, save_last_tiff)


def test_integrate_core_smoke(exp_db, fast_tmp_dir):
    old_files = os.listdir(fast_tmp_dir)
    old_times = [os.path.getmtime(os.path.join(fast_tmp_dir, f)) for f in
                 os.listdir(fast_tmp_dir)]
    integrate_and_save(exp_db[-1], db=exp_db, save_dir=fast_tmp_dir,
                       mask_setting=None)
    assert (set(old_files) != set(os.listdir(fast_tmp_dir)) or set(
        old_times) != set(
        [os.path.getmtime(os.path.join(fast_tmp_dir, f)) for f in
         os.listdir(fast_tmp_dir)]))


def test_integrate_last_core_smoke(exp_db, fast_tmp_dir):
    old_files = os.listdir(fast_tmp_dir)
    old_times = [os.path.getmtime(os.path.join(fast_tmp_dir, f)) for f in
                 os.listdir(fast_tmp_dir)]
    integrate_and_save_last(db=exp_db, save_dir=fast_tmp_dir,
                            mask_setting=None)
    assert (set(old_files) != set(os.listdir(fast_tmp_dir)) or set(
        old_times) != set(
        [os.path.getmtime(os.path.join(fast_tmp_dir, f)) for f in
         os.listdir(fast_tmp_dir)]))


def test_save_tiff_core_smoke(exp_db, fast_tmp_dir):
    old_files = os.listdir(fast_tmp_dir)
    old_times = [os.path.getmtime(os.path.join(fast_tmp_dir, f)) for f in
                 os.listdir(fast_tmp_dir)]
    save_tiff(exp_db[-1], db=exp_db, save_dir=fast_tmp_dir)

    assert (
        set(old_files) != set(os.listdir(fast_tmp_dir)) or set(
            old_times) != set(
            [os.path.getmtime(os.path.join(fast_tmp_dir, f)) for f in
             os.listdir(fast_tmp_dir)]))


def test_save_last_tiff_core_smoke(exp_db, fast_tmp_dir):
    old_files = os.listdir(fast_tmp_dir)
    old_times = [os.path.getmtime(os.path.join(fast_tmp_dir, f)) for f in
                 os.listdir(fast_tmp_dir)]
    save_last_tiff(db=exp_db, save_dir=fast_tmp_dir)

    assert (
        set(old_files) != set(os.listdir(fast_tmp_dir)) or set(
            old_times) != set(
            [os.path.getmtime(os.path.join(fast_tmp_dir, f)) for f in
             os.listdir(fast_tmp_dir)]))
