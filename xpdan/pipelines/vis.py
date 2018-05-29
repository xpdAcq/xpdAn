from bluesky.callbacks.broker import LiveImage
from shed.translation import ToEventStream
from xpdan.pipelines.main import *

from xpdview.callbacks import LiveWaterfall

# Visualization
# background corrected img
ToEventStream(bg_corrected_img,
              ('image',)).starsink(
    LiveImage('image', window_title='Background_corrected_img',
              cmap='viridis'))

# polarization corrected img with mask overlayed
ToEventStream(
    pol_corrected_img.combine_latest(mask).starmap(overlay_mask),
    ('image',)).starsink(LiveImage('image', window_title='final img',
                                   limit_func=lambda im: (
                                       np.nanpercentile(im, 2.5),
                                       np.nanpercentile(im, 97.5)
                                   ), cmap='viridis'))

# integrated intensities
iq_em = (ToEventStream(mean
                       .combine_latest(q, emit_on=0), ('iq', 'q'))
         .starsink(LiveWaterfall('q', 'iq', units=('1/A', 'Intensity'),
                                 window_title='{} vs {}'.format('iq', 'q')),
                   stream_name='{} {} vis'.format('q', 'iq')))
(ToEventStream(mean
               .combine_latest(tth, emit_on=0), ('iq', 'tth'))
 .starsink(LiveWaterfall('tth', 'iq', units=('Degree', 'Intensity'),
                         window_title='{} vs {}'.format('iq', 'tth')),
           stream_name='{} {} vis'.format('tth', 'iq')))
# F(Q)
ToEventStream(fq, ('q', 'fq')).starsink(
    LiveWaterfall('q', 'fq', units=('1/A', 'Intensity'),
                  window_title='F(Q)'), stream_name='F(Q) vis')
# PDF
ToEventStream(pdf, ('r', 'gr')).starsink(
    LiveWaterfall('r', 'gr', units=('A', '1/A**2'),
                  window_title='PDF'), stream_name='G(r) vis')
