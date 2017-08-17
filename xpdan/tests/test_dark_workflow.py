import os
from xpdan.pipelines.dark_workflow import source, dark_template_stream


def test_dark_workflow(exp_db):
    dark_hdr = next(iter(exp_db(is_dark={'$exists': True})))
    L1 = dark_template_stream.sink_to_list()

    for nd in dark_hdr.documents(fill=True):
        source.emit(nd)
    for name, doc in L1:
        if name == 'event':
            assert os.path.exists(doc['data']['file_path'])
