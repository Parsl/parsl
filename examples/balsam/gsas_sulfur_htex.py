import logging

logging.basicConfig(
    format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

logger = logging.getLogger()

import os
import parsl
from parsl.config import Config
from parsl.app.app import python_app, container_app
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


#@container_app(type="singularity", image="/home/darren/alcf/singularity/git/gsas2container/gsas2.img", data="/home/darren/alcf/singularity/work/data", cmd="/home/darren/alcf/singularity/git/singularity/builddir/singularity")
@python_app(executors=['local_htex'])
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

    ceo2Image.loadControls('ceo2.imctrl')
    capillaryImage.loadControls('capillary.imctrl')
    sulfurImage.loadControls('sulfur.imctrl')

    sulfurPWDRList = sulfurImage.Integrate()
    capillaryPWDRList = capillaryImage.Integrate()

    sulfurPWDRList[0].SaveProfile('pwdr_Sulfur')
    capillaryPWDRList[0].SaveProfile('pwdr_Capillary')

    print(sulfurPWDRList)

    pdf = gpx.add_PDF('pwdr_Sulfur.instprm', 0)

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
    'filename': 'sulfur-test.gpx',
    'outputfig': 'sulfur.png',
    'export': {
        'prefix': 'Sulfur_At2_413K-00147'
    },
    'limits': {
        'xlim': [0.012914191411475429, 24.096733130017093],
        'ylim': [-1.937982691074275, 4.881099529628997]
    },
    'images': {
        'CeO2': 'CeO2_0p1s30f-00001.tif',
        'Capillary': 'Capillary_413K-00471.tif',
        'Sulfur': 'Sulfur_At2_413K-00147.tif'
    },
    'controls': {
        'CeO2': 'ceo2.imctrl',
        'Capillary': 'capillary.imctrl',
        'Sulfur': 'sulfur.imctrl'
    }
}

pdf = PDF_Workflow(inputs=[data])

print("RESULT:", pdf.result())
