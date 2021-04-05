import parsl
import os
from parsl.app.app import python_app
from parsl.config import Config
from parsl.executors.balsam.executor import BalsamExecutor


config = Config(
    executors=[
        BalsamExecutor(
            siteid=1,
            maxworkers=3,
            numnodes=1,
            timeout=60,
            node_packing_count=8,
            sitedir='git/site1',
            project='local'
        )
    ]
)

parsl.load(config)


def callback(future, **kwargs):

    if not future.cancelled():
        print('Callback result: ', future.result())
    else:
        print('Future was cancelled!')


@python_app(executors=['BalsamExecutor'])
def HistStats(inputs=[]):
    import os
    import sys
    sys.path.insert(0, '/home/darren/gsas')
    import GSASIIscriptable as G2sc

    filename = inputs[0]
    gpx = G2sc.G2Project(gpxfile=filename)
    '''prints profile rfactors for all histograms'''
    print(u"*** profile Rwp, " + os.path.split(filename)[1])
    for hist in gpx.histograms():
        print("\t{:20s}: {:.2f}".format(hist.name, hist.get_wR()))
    gpx.save()


@python_app(executors=['BalsamExecutor'])
def CreateHistograms(inputs=[]):
    import os
    import sys
    sys.path.insert(0, '/home/darren/gsas')
    import GSASIIscriptable as G2sc

    datadir = "/home/darren/gsas/svn/pyGSAS/Tutorials/PythonScript/data"
    # create a project with a default project name
    gpx = G2sc.G2Project(filename='PbSO4.gpx')

    # setup step 1: add two histograms to the project
    hist1 = gpx.add_powder_histogram(os.path.join(datadir, "PBSO4.XRA"),
                                     os.path.join(datadir, "INST_XRY.PRM"))
    hist2 = gpx.add_powder_histogram(os.path.join(datadir, "PBSO4.CWN"),
                                     os.path.join(datadir, "inst_d1a.prm"))
    # setup step 2: add a phase and link it to the previous histograms
    phase0 = gpx.add_phase(os.path.join(datadir, "PbSO4-Wyckoff.cif"),
                           phasename="PbSO4",
                           histograms=[hist1, hist2])

    print('Filename: '+os.path.abspath('PbSO4.gpx'))
    return os.path.abspath('PbSO4.gpx')


@python_app(executors=['BalsamExecutor'])
def RefineGPX(inputs=[]):
    import sys
    sys.path.insert(0, '/home/darren/gsas')
    import GSASIIscriptable as G2sc

    filename = inputs[0]
    print('Filename: _'+filename+'_')
    gpx = G2sc.G2Project(gpxfile=filename)
    # not in tutorial: increase # of cycles to improve convergence
    gpx.data['Controls']['data']['max cyc'] = 8  # not in API
    # tutorial step 4: turn on background refinement (Hist)
    refdict0 = {"set": {"Background": {"no. coeffs": 3, "refine": True}}}
    gpx.save('step4.gpx')
    gpx.do_refinements([refdict0])

    return gpx.filename


create_histograms = CreateHistograms(callback=callback)
refine_gpx = RefineGPX(callback=callback, inputs=[create_histograms])

hist_stats = HistStats(callback=callback, inputs=[refine_gpx])

print(hist_stats.result())
