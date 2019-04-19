from bluesky.callbacks import CallbackBase
from mayavi import mlab as mlab


class Live3DView(CallbackBase):
    """Callback for visualizing 3D data """

    def __init__(self):
        self.cs_dict = {}
        self.source_dict = {}
        self.fields = []
        self.pipeline_dict = {}

    def start(self, doc):
        self.cs_dict = {}
        self.source_dict = {}
        self.fields = []
        self.pipeline_dict = {}

    def descriptor(self, doc):

        self.fields = [
            k
            for k, v in doc["data_keys"].items()
            if len(v["shape"]) == 3 and all(dim > 0 for dim in v["shape"])
        ]
        for field in self.fields:
            fig = mlab.figure(field)
            mlab.clf(fig)
            self.cs_dict[field] = fig
            self.source_dict[field] = None
            self.pipeline_dict[field] = []

    def event(self, doc):

        for field in self.fields:
            data = doc["data"][field]
            figure = self.cs_dict[field]
            x = self.source_dict[field]
            # Don't plot data which is (N, M, 1) because Mayavi doesn't like it
            if data.shape[-1] != 1:
                if x is None:
                    x = mlab.pipeline.scalar_field(data, figure=figure)
                    self.source_dict[field] = x
                    for i, orientation in enumerate("xyz"):
                        self.pipeline_dict[field].append(
                            mlab.pipeline.image_plane_widget(
                                x,
                                plane_orientation=f"{orientation}_axes",
                                slice_index=data.shape[i] // 2,
                                figure=figure,
                            )
                        )
                    mlab.pipeline.volume(x, figure=figure)
                else:
                    x.mlab_source.scalars = data
                    for p in self.pipeline_dict[field]:
                        sl = p.ipw.slice_index
                        p.update_pipeline()
                        p.ipw.slice_index = sl
