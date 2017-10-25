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
from xpdan.tools import (generate_binner, load_geo,
                         polarization_correction, mask_img, pdf_getter,
                         fq_getter, overlay_mask)
from xpdview.callbacks import LiveWaterfall


def format_event(**kwargs):
    return {'data': dict(**kwargs),
            'filled': {k: True for k in kwargs}}


class MainCallback(CallbackBase):
    def __init__(self, db, save_dir, *,
                 polarization_factor=.99,
                 image_data_key='pe1_image',
                 mask_kwargs=None,
                 fq_kwargs=None,
                 pdf_kwargs=None,
                 calibration_md_folder='../xpdConfig/'):
        if mask_kwargs is None:
            mask_kwargs = {}
        _pdf_config = dict(dataformat='QA', qmaxinst=28, qmax=22)
        _fq_config = dict(dataformat='QA', qmaxinst=30, qmax=30)
        if pdf_kwargs is None:
            pdf_kwargs = _pdf_config.copy()
        else:
            pdf_config = _pdf_config.copy()
            pdf_config.update(pdf_kwargs)
            pdf_kwargs = pdf_config

        if fq_kwargs is None:
            fq_kwargs = _fq_config.copy()
        else:
            fq_config = _fq_config.copy()
            fq_config.update(fq_kwargs)
            fq_kwargs = fq_config

        self.image_data_key = image_data_key
        self.mask_kwargs = mask_kwargs
        self.calibration_md_folder = calibration_md_folder
        self.pdf_kwargs = pdf_kwargs
        self.fq_kwargs = fq_kwargs
        self.polarization_factor = polarization_factor
        self.db = db
        self.save_dir = save_dir
        self.light_template = os.path.join(self.save_dir, base_template)
        self.vis_callbacks = {'dark_sub_iq': LiveImage(
            'img', window_title='Dark Subtracted Image', cmap='viridis'),
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
                                 window_title='G(r)')
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
        yml_name = render_and_clean(self.light_template, ext='.yml',
                                    raw_start=doc)
        dump_yml(yml_name, doc)
        self.start_doc = doc
        self.wavelength = doc.get('bt_wavelength')
        self.composition = doc.get('composition_string')
        is_dark = if_dark(doc)
        # If the data is not a dark
        if not is_dark:
            dark = query_dark(self.db, [doc])

            # If there is a dark associated
            if dark:
                dark = temporal_prox(dark, [doc])[0]
                self.dark_img = next(dark.data(self.image_data_key))
            background = query_background(self.db, [doc])

            # If there is a background associated
            if background:
                background = temporal_prox(background, [doc])[0]
                self.background_img = next(
                    background.data(self.image_data_key))
                bg_dark = query_dark(self.db, [background])

                if bg_dark:
                    bg_dark = temporal_prox(bg_dark, [doc])
                    bg_dark_img = next(bg_dark.data(self.image_data_key))
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
        for k, v in self.vis_callbacks.items():
            v('start', doc)

    def descriptor(self, doc):
        self.descs.append(doc)
        self.descriptor_doc = doc
        # Run all the descriptor callbacks
        for k, v in self.vis_callbacks.items():
            v('descriptor', doc)

    def event(self, doc):
        if self.dark_img is not None:
            doc = next(self.db.fill_events([doc], self.descs))
            # human readable timestamp
            h_timestamp = _timestampstr(doc['time'])

            # dark subtraction
            img = doc['data'][self.image_data_key]
            if self.dark_img is not None:
                img -= self.dark_img
            tiff_name = render_and_clean(self.light_template,
                                         human_timestamp=h_timestamp,
                                         raw_event=doc,
                                         raw_start=self.start_doc,
                                         raw_descriptor=self.descriptor_doc,
                                         analyzed_start={'analysis_stage':
                                                             'dark_sub'},
                                         ext='.tiff')
            self.vis_callbacks['dark_sub_iq']('event', format_event(img=img))
            tifffile.imsave(tiff_name, img)

            # background correction
            if self.background_img:
                img -= self.background_img

            # get calibration
            if self.is_calibration:
                calibration, geo = img_calibration(img, self.wavelength,
                                                   self.calibrant,
                                                   self.detector)
                poni_name = render_and_clean(
                    self.light_template,
                    human_timestamp=h_timestamp,
                    raw_event=doc,
                    raw_start=self.start_doc,
                    raw_descriptor=self.descriptor_doc,
                    analyzed_start={'analysis_stage':
                                        'calib'},
                    ext='.poni')
                poni_saver(poni_name, calibration)
                _save_calib_param(calibration, h_timestamp,
                                  os.path.join(self.calibration_md_folder,
                                               'xpdAcq_calib_info.yml'))
            elif self.calibrant:
                geo = load_geo(self.calibrant)
            else:
                geo = None

            if geo:
                img = polarization_correction(img, geo,
                                              self.polarization_factor)

                # Masking
                if doc['seq_num'] == 1:
                    if self.start_doc['sample_name'] == 'Setup':
                        self.mask = np.ones(img.shape, dtype=bool)
                    else:
                        self.mask = mask_img(img, geo, **self.mask_kwargs)
                        mask_name = render_and_clean(
                            self.light_template,
                            human_timestamp=h_timestamp,
                            raw_event=doc,
                            raw_start=self.start_doc,
                            raw_descriptor=self.descriptor_doc,
                            analyzed_start={
                                'analysis_stage': 'mask'},
                            ext='')
                        fit2d_save(self.mask, mask_name)
                overlay = overlay_mask(img, self.mask)
                self.vis_callbacks['masked_img']('event',
                                                 format_event(
                                                     overlay_mask=overlay)
                                                 )
                # binner
                binner = generate_binner(geo, img.shape, self.mask)

                q, iq = binner.bin_centers, np.nan_to_num(
                    binner(img.flatten()))
                iq_name = render_and_clean(self.light_template,
                                           human_timestamp=h_timestamp,
                                           raw_event=doc,
                                           raw_start=self.start_doc,
                                           raw_descriptor=self.descriptor_doc,
                                           analyzed_start={
                                               'analysis_stage': 'iq_q'},
                                           ext='_Q.chi')
                self.vis_callbacks['iq']('event', format_event(q=q, iq=iq)
                                         )
                save_output(q, iq, iq_name, 'Q')
                tth = np.rad2deg(q_to_twotheta(q, self.wavelength))
                itth_name = render_and_clean(
                    self.light_template,
                    human_timestamp=h_timestamp,
                    raw_event=doc,
                    raw_start=self.start_doc,
                    raw_descriptor=self.descriptor_doc,
                    analyzed_start={
                        'analysis_stage': 'iq_tth'},
                    ext='_tth.chi')
                self.vis_callbacks['itth']('event',
                                           format_event(tth=tth, iq=iq))
                save_output(tth, iq, itth_name, '2theta')

                if self.composition:
                    fq_q, fq, fq_config = fq_getter(
                        q, iq,
                        composition=self.composition,
                        **self.fq_kwargs)
                    self.vis_callbacks['fq']('event',
                                             format_event(q=fq_q, fq=fq))

                    r, gr, pdf_config = pdf_getter(
                        q, iq,
                        composition=self.composition,
                        **self.pdf_kwargs)
                    self.vis_callbacks['pdf']('event',
                                              format_event(r=r, pdf=gr)
                                              )
                    pdf_name = render_and_clean(
                        self.light_template,
                        human_timestamp=h_timestamp,
                        raw_event=doc,
                        raw_start=self.start_doc,
                        raw_descriptor=self.descriptor_doc,
                        analyzed_start={
                            'analysis_stage': 'pdf'},
                        ext='.gr')
                    pdf_saver(r, gr, pdf_name, pdf_config)

    def stop(self, doc):
        for k, v in self.vis_callbacks.items():
            v('stop', doc)
