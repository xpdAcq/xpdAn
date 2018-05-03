from xpdan.pipelines.main import *  # noqa: F403, F401
from bluesky.callbacks.broker import LiveImage
from shed.translation import ToEventStream
from xpdtools.pipelines.extra import z_score

# Zscore
z_score_plot = ToEventStream(z_score, ('z_score',)).starsink(
    LiveImage('z_score', cmap='viridis', window_title='z score',
              limit_func=lambda im: (-2, 2)), stream_name='z score vis')
