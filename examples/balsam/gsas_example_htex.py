import parsl
from parsl.app.app import python_app
from parsl.config import Config
from parsl.app.app import python_app, singularity_app
from parsl.providers.local.local import LocalProvider
from parsl.executors import HighThroughputExecutor

config = Config(
    executors=[
        HighThroughputExecutor(
            label='local_htex',
            max_workers=2,
            address='0.0.0.0',
            provider=LocalProvider(
                min_blocks=1,
                init_blocks=1,
                max_blocks=2,
                nodes_per_block=1,
                parallelism=0.5
            )
        )
    ]
)

parsl.load(config)


def callback(future, **kwargs):
    if not future.cancelled():
        print('Callback result: ', future.result())
    else:
        print('Future was cancelled!')


@singularity_app(image="/home/darren/alcf/singularity/git/gsas2container/gsas2.img", cmd="/home/darren/alcf/singularity/git/singularity/builddir/singularity")
@python_app(executors=['BalsamExecutor'])
def HistStats(inputs=[]):
    import os
    import sys
    sys.path.insert(0, '/work/gsas2/GSASII')
    import GSASIIscriptable as G2sc

    filename = inputs[0]
    gpx = G2sc.G2Project(gpxfile=filename)
    '''prints profile rfactors for all histograms'''
    print(u"*** profile Rwp, " + os.path.split(filename)[1])
    for hist in gpx.histograms():
        print("\t{}: {}".format(hist.name, hist.get_wR()))
    gpx.save()


@singularity_app(image="/home/darren/alcf/singularity/git/gsas2container/gsas2.img", cmd="/home/darren/alcf/singularity/git/singularity/builddir/singularity")
@python_app(executors=['BalsamExecutor'])
def CreateHistograms(inputs=[]):
    import os
    import sys
    sys.path.insert(0, '/work/gsas2/GSASII')
    import GSASIIscriptable as G2sc

    datadir = "/app"
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

    print('Filename: ' + os.path.abspath('PbSO4.gpx'))
    return os.path.abspath('PbSO4.gpx')


@singularity_app(image="/home/darren/alcf/singularity/git/gsas2container/gsas2.img", cmd="/home/darren/alcf/singularity/git/singularity/builddir/singularity")
@python_app(executors=['BalsamExecutor'])
def RefineGPX(inputs=[]):
    import sys
    sys.path.insert(0, '/work/gsas2/GSASII')
    import GSASIIscriptable as G2sc

    filename = inputs[0]
    print('Filename: _' + filename + '_')
    gpx = G2sc.G2Project(gpxfile=filename)
    # not in tutorial: increase # of cycles to improve convergence
    gpx.data['Controls']['data']['max cyc'] = 8  # not in API
    # tutorial step 4: turn on background refinement (Hist)
    refdict0 = {"set": {"Background": {"no. coeffs": 3, "refine": True}}}
    gpx.save('step4.gpx')
    gpx.do_refinements([refdict0])

    return gpx.filename


create_histograms = CreateHistograms()
refine_gpx = RefineGPX(inputs=[create_histograms])
hist_stats = HistStats(inputs=[refine_gpx])

print(hist_stats.result())
