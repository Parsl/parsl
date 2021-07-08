import parsl
from parsl.config import Config
from parsl.app.app import python_app
from parsl.executors.balsam.executor import BalsamExecutor
import glob

config = Config(
    executors=[
        BalsamExecutor(
            siteid=5,
            maxworkers=3,
            image="/home/darren/alcf/singularity/git/gsas2container/gsas2.img",
            datadir="/home/darren/alcf/singularity/git/gsas2container/gsas/example",
            numnodes=1,
            timeout=60,
            node_packing_count=6,
            sitedir='/home/darren/alcf/singularity/git/gsas2container/site3',
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
def PDF_Workflow(inputs=[]):
    import numpy as np
    import matplotlib.pyplot as plt
    from matplotlib.axis import Axis
    import GSASIIscriptable as G2sc

    # Create workflow GSAS2 project
    data = inputs[0]

    filename = data['filename']
    ceo2ImageFile = data['images']['CeO2']
    capillaryImageFile = data['images']['Capillary']
    sulfurImageFile = data['images']['Sulfur']

    # Create workflow GSAS2 project
    gpx = G2sc.G2Project(filename=filename)

    # Load tif images
    gpx.add_image(ceo2ImageFile)
    gpx.add_image(capillaryImageFile)
    gpx.add_image(sulfurImageFile)

    ceo2Image = gpx.images()[0]
    capillaryImage = gpx.images()[1]
    sulfurImage = gpx.images()[2]

    ceo2Image.loadControls('/data/ceo2.imctrl')
    capillaryImage.loadControls('/data/capillary.imctrl')
    sulfurImage.loadControls('/data/sulfur.imctrl')

    sulfurPWDRList = sulfurImage.Integrate()
    capillaryPWDRList = capillaryImage.Integrate()

    sulfurPWDRList[0].SaveProfile('pwdr_Sulfur')
    capillaryPWDRList[0].SaveProfile('pwdr_Capillary')

    print(sulfurPWDRList)

    pdf = gpx.add_PDF('/data/pwdr_Sulfur.instprm', 0)

    pdf.set_formula(['S', 1])
    pdf.data['PDF Controls']['Container']['Name'] = capillaryPWDRList[0].name
    pdf.data['PDF Controls']['Container']['Mult'] = -0.988
    pdf.data['PDF Controls']['Form Vol'] = 13.306
    pdf.data['PDF Controls']['Geometry'] = 'Cylinder'
    pdf.data['PDF Controls']['DetType'] = 'Area Detector'

    # IP transmission coef?
    pdf.data['PDF Controls']['ObliqCoeff'] = 0.2

    pdf.data['PDF Controls']['Flat Bkg'] = 5081
    pdf.data['PDF Controls']['BackRatio'] = 1.0
    pdf.data['PDF Controls']['Ruland'] = 0.1
    pdf.data['PDF Controls']['Lorch'] = True
    pdf.data['PDF Controls']['QScaleLim'] = [23.4, 26.0]
    pdf.data['PDF Controls']['Pack'] = 1.0
    pdf.data['PDF Controls']['Diam'] = 1.5
    pdf.data['PDF Controls']['Trans'] = 0.2
    pdf.data['PDF Controls']['Ruland'] = 0.1
    pdf.data['PDF Controls']['BackRatio'] = 0.184
    pdf.data['PDF Controls']['Rmax'] = 20.0

    pdf.calculate()
    pdf.optimize()
    pdf.optimize()
    pdf.optimize()
    pdf.export(data['export']['prefix'], 'I(Q), S(Q), F(Q), G(r)')

    gpx.save()

    x, y = np.loadtxt(data['export']['prefix']+'.gr', unpack=True)

    plt.plot(x, y, label='Sulfur')
    fig = plt.figure(1)

    plt.title('Sulfur')
    plt.xlabel('x')
    plt.ylabel('y')

    axes = plt.gca()

    plt.xlim(data['limits']['xlim'])
    plt.ylim(data['limits']['ylim'])

    plt.savefig(data['outputfig'])

    return data


data = {
    'filename': '/data/sulfur-test.gpx',
    'outputfig': '/data/sulfur.png',
    'export': {
        'prefix': '/data/Sulfur_At2_413K-00147'
    },
    'limits': {
        'xlim': [0.012914191411475429, 24.096733130017093],
        'ylim': [-1.937982691074275, 4.881099529628997]
    },
    'images': {
        'CeO2': '/data/CeO2_0p1s30f-00001.tif',
        'Capillary': '/data/Capillary_413K-00471.tif',
        'Sulfur': '/data/Sulfur_At2_413K-00147.tif'
    },
    'controls': {
        'CeO2': '/data/ceo2.imctrl',
        'Capillary': '/data/capillary.imctrl',
        'Sulfur': '/data/sulfur.imctrl'
    }
}


pdf = PDF_Workflow(inputs=[data])

print("RESULT:", pdf.result())
