import os

import numpy as np
import tifffile

from bluesky.callbacks.broker import LiveImage
from bluesky.callbacks.core import CallbackBase
from skbeam.core.utils import q_to_twotheta
from skbeam.io.fit2d import fit2d_save
from skbeam.io.save_powder_output import save_output
from xpdan.calib import img_calibration, _save_calib_param
from xpdan.db_utils import query_dark, temporal_prox, query_background
from xpdan.dev_utils import _timestampstr
from xpdan.formatters import render_and_clean
from xpdan.io import pdf_saver, dump_yml, poni_saver
from xpdan.pipelines.pipeline_utils import (if_dark, if_calibration,
                                            base_template)
from xpdtools.tools import (generate_binner, load_geo,
                            polarization_correction, mask_img, pdf_getter,
                            fq_getter, overlay_mask, z_score_image)
from xpdview.callbacks import LiveWaterfall


def format_event(**kwargs):
    return {'data': dict(**kwargs),
            'filled': {k: True for k in kwargs}}


def render_clean_makedir(string, **kwargs):
    rendered_string = render_and_clean(string, **kwargs)
    os.makedirs(os.path.split(rendered_string)[0], exist_ok=True)
    return rendered_string


class MainCallback(CallbackBase):
    def __init__(self, db, save_dir, *,
                 vis=True,
                 write_to_disk=True,
                 polarization_factor=.99,
                 image_data_key='pe1_image',
                 mask_kwargs=None,
                 fq_config=None,
                 pdf_config=None,
                 calibration_md_folder='../xpdConfig/',
                 mask_setting='default',
                 analysis_setting='full'):
        self.vis = vis
        self.write_to_disk = write_to_disk
        self.analysis_setting = analysis_setting
        self.mask_setting = mask_setting
        if mask_kwargs is None:
            mask_kwargs = {}
        _pdf_config = dict(dataformat='QA', qmaxinst=28, qmax=22)
        _fq_config = dict(dataformat='QA', qmaxinst=26, qmax=25)
        if pdf_config is None:
            pdf_config = _pdf_config.copy()
        else:
            pdf_config2 = _pdf_config.copy()
            pdf_config2.update(pdf_config)
            pdf_config = pdf_config2

        if fq_config is None:
            fq_config = _fq_config.copy()
        else:
            fq_config2 = _fq_config.copy()
            fq_config2.update(fq_config)
            fq_config = fq_config2

        self.image_data_key = image_data_key
        self.mask_kwargs = mask_kwargs
        self.calibration_md_folder = calibration_md_folder
        self.pdf_kwargs = pdf_config
        self.fq_kwargs = fq_config
        self.polarization_factor = polarization_factor
        self.db = db
        self.save_dir = save_dir
        self.light_template = os.path.join(self.save_dir, base_template)
        if self.vis:
            self.vis_callbacks = {
                'dark_sub_iq': LiveImage('img',
                                         window_title='Dark Subtracted Image',
                                         cmap='viridis'),
                'masked_img': LiveImage('overlay_mask',
                                        window_title='Dark/Background/'
                                                     'Polarization Corrected '
                                                     'Image with Mask',
                                        cmap='viridis',
                                        limit_func=lambda im: (
                                            np.nanpercentile(im, 1),
                                            np.nanpercentile(im, 99))
                                        # norm=LogNorm()
                                        ),
                'iq': LiveWaterfall('q', 'iq',
                                    units=('Q (A^-1)', 'Arb'),
                                    window_title='I(Q)'),
                'itth': LiveWaterfall('tth', 'iq',
                                      units=('tth', 'Arb'),
                                      window_title='I(tth)'),
                'fq': LiveWaterfall('q', 'fq',
                                    units=('Q (A^-1)', 'F(Q)'),
                                    window_title='F(Q)'),
                'pdf': LiveWaterfall('r', 'pdf',
                                     units=('r (A)', 'G(r) A^-2'),
                                     window_title='G(r)'),
                'zscore': LiveImage('img',
                                    window_title='Z Score Image',
                                    cmap='viridis')
            }

        self.start_doc = None
        self.descriptor_doc = None
        self.mask = None
        self.composition = None
        self.wavelength = None
        self.dark_img = None
        self.background_img = None
        self.is_calibration = None
        self.detector = None
        self.calibrant = None
        self.descs = None

    def start(self, doc):
        self.dark_img = None
        self.background_img = None

        self.descs = []
        self.start_doc = doc
        self.wavelength = doc.get('bt_wavelength')
        self.composition = doc.get('composition_string')
        is_dark = if_dark(doc)
        # If the data is not a dark
        if not is_dark:
            if self.write_to_disk:
                yml_name = render_clean_makedir(self.light_template,
                                                ext='.yml',
                                                raw_start=doc)
                dump_yml(yml_name, doc)
            dark = query_dark([doc], self.db)

            # If there is a dark associated
            if dark:
                dark = temporal_prox(dark, [doc])[0]
                self.dark_img = next(dark.data(self.image_data_key))
                if str(self.dark_img.dtype) == 'uint16':
                    self.dark_img = self.dark_img.astype('float32')
            background = query_background([doc], self.db)

            # If there is a background associated
            if background:
                background = temporal_prox(background, [doc])[0]
                self.background_img = next(
                    background.data(self.image_data_key))
                if str(self.background_img.dtype) == 'uint16':
                    self.background_img = self.background_img.astype('float32')
                bg_dark = query_dark([background['start']], self.db)

                if bg_dark:
                    bg_dark = temporal_prox(bg_dark, [doc])[0]
                    bg_dark_img = next(bg_dark.data(self.image_data_key))
                    if str(bg_dark_img.dtype) == 'uint16':
                        bg_dark_img = bg_dark_img.astype('float32')
                else:
                    bg_dark_img = np.zeros(self.background_img.shape)
                self.background_img = self.background_img - bg_dark_img

            # If this is calibration data
            self.is_calibration = if_calibration(doc)
            if self.is_calibration:
                self.calibrant = doc.get('dSpacing')
                self.detector = doc.get('detector')
            else:
                self.calibrant = doc.get('calibration_md')
        # Run all the starts for the callbacks
        if self.vis:
            for k, v in self.vis_callbacks.items():
                v('start', doc)

    def descriptor(self, doc):
        if 'cryostat_T' in doc['data_keys']:
            doc['data_keys']['cryostat_T']['units'] = 'K'
            # rename to temperature
            doc['data_keys']['temperature'] = doc['data_keys'].pop(
                'cryostat_T')
        self.descs.append(doc)
        self.descriptor_doc = doc
        # Run all the descriptor callbacks
        if self.vis:
            for k, v in self.vis_callbacks.items():
                v('descriptor', doc)

    def event(self, doc):
        if self.dark_img is not None:
            doc = next(self.db.fill_events([doc], self.descs))
            if 'cryostat_T' in doc['data']:
                doc['data']['temperature'] = doc['data'].pop('cryostat_T')
            # human readable timestamp
            h_timestamp = _timestampstr(doc['time'])

            # dark subtraction
            img = doc['data'][self.image_data_key]
            if str(img.dtype) == 'uint16':
                img = img.astype('float32')
            if self.dark_img is not None:
                img -= self.dark_img
            if self.vis:
                self.vis_callbacks['dark_sub_iq']('event',
                                                  format_event(img=img))
            if self.write_to_disk:
                tiff_name = render_clean_makedir(
                    self.light_template,
                    human_timestamp=h_timestamp,
                    raw_event=doc,
                    raw_start=self.start_doc,
                    raw_descriptor=self.descriptor_doc,
                    analysis_stage='dark_sub',
                    ext='.tiff')
                tifffile.imsave(tiff_name, img)

            if self.analysis_setting == 'full':
                # background correction
                if self.background_img is not None:
                    img -= self.background_img

                # get calibration
                if self.is_calibration:
                    calibration, geo = img_calibration(img, self.wavelength,
                                                       self.calibrant,
                                                       self.detector)
                    _save_calib_param(calibration, h_timestamp,
                                      os.path.join(self.calibration_md_folder,
                                                   'xpdAcq_calib_info.yml'))
                    if self.write_to_disk:
                        poni_name = render_clean_makedir(
                            self.light_template,
                            human_timestamp=h_timestamp,
                            raw_event=doc,
                            raw_start=self.start_doc,
                            raw_descriptor=self.descriptor_doc,
                            analysis_stage='calib',
                            ext='.poni')
                        poni_saver(poni_name, calibration)

                elif self.calibrant:
                    geo = load_geo(self.calibrant)
                else:
                    geo = None

                if geo:
                    img = polarization_correction(img, geo,
                                                  self.polarization_factor)

                    # Masking
                    if doc['seq_num'] == 1:
                        if (self.start_doc['sample_name'] == 'Setup' or
                                self.mask_setting is None):
                            self.mask = np.ones(img.shape, dtype=bool)
                        else:
                            binner = generate_binner(geo, img.shape, self.mask)
                            self.mask = mask_img(img, binner,
                                                 **self.mask_kwargs)
                        if self.write_to_disk:
                            mask_name = render_clean_makedir(
                                self.light_template,
                                human_timestamp=h_timestamp,
                                raw_event=doc,
                                raw_start=self.start_doc,
                                raw_descriptor=self.descriptor_doc,
                                analysis_stage='mask',
                                ext='')
                            fit2d_save(self.mask, mask_name)
                    if self.vis:
                        overlay = overlay_mask(img, self.mask)
                        self.vis_callbacks['masked_img'](
                            'event', format_event(overlay_mask=overlay))
                    # binner
                    binner = generate_binner(geo, img.shape, self.mask)

                    q, iq = binner.bin_centers, np.nan_to_num(
                        binner(img.flatten()))
                    if self.vis:
                        self.vis_callbacks['iq']('event',
                                                 format_event(q=q, iq=iq))
                        self.vis_callbacks['zscore']('event',
                                                     format_event(
                                                         img=z_score_image(
                                                             img, binner)))
                    if self.write_to_disk:
                        iq_name = render_clean_makedir(
                            self.light_template,
                            human_timestamp=h_timestamp,
                            raw_event=doc,
                            raw_start=self.start_doc,
                            raw_descriptor=self.descriptor_doc,
                            analysis_stage='iq_q',
                            ext='_Q.chi')
                        save_output(q, iq, iq_name, 'Q')
                    tth = np.rad2deg(q_to_twotheta(q, self.wavelength))
                    if self.vis:
                        self.vis_callbacks['itth']('event',
                                                   format_event(tth=tth,
                                                                iq=iq))
                    if self.write_to_disk:
                        itth_name = render_clean_makedir(
                            self.light_template,
                            human_timestamp=h_timestamp,
                            raw_event=doc,
                            raw_start=self.start_doc,
                            raw_descriptor=self.descriptor_doc,
                            analysis_stage='iq_tth',
                            ext='_tth.chi')

                        save_output(tth, iq, itth_name, '2theta')

                    if self.composition:
                        fq_q, fq, fq_config = fq_getter(
                            q, iq,
                            composition=self.composition,
                            **self.fq_kwargs)
                        if self.vis:
                            self.vis_callbacks['fq'](
                                'event', format_event(q=fq_q, fq=fq))

                        r, gr, pdf_config = pdf_getter(
                            q, iq,
                            composition=self.composition,
                            **self.pdf_kwargs)
                        if self.vis:
                            self.vis_callbacks['pdf']('event',
                                                      format_event(r=r, pdf=gr)
                                                      )
                        if self.write_to_disk:
                            pdf_name = render_clean_makedir(
                                self.light_template,
                                human_timestamp=h_timestamp,
                                raw_event=doc,
                                raw_start=self.start_doc,
                                raw_descriptor=self.descriptor_doc,
                                analysis_stage='pdf',
                                ext='.gr')
                            pdf_saver(r, gr, pdf_config, pdf_name)

    def stop(self, doc):
        if self.vis:
            for k, v in self.vis_callbacks.items():
                v('stop', doc)
