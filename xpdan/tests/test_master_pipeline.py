from xpdan.pipelines.master import conf_master_pipeline
import matplotlib.pyplot as plt


def test_master_pipeline(exp_db):
    """Decider between pipelines"""

    source = conf_master_pipeline(exp_db)
    for nd in exp_db[-1].documents(fill=True):
        source.emit(nd)
    # plt.show()
