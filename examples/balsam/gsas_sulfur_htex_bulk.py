import time
import datetime
import os
import parsl

from parsl.config import Config
from parsl.app.app import python_app, container_app
from parsl.providers.local.local import LocalProvider
from parsl.executors import HighThroughputExecutor
import glob


config = Config(
    executors=[
        HighThroughputExecutor(
            label='local_htex',
            max_workers=12,
            address='0.0.0.0',
            provider=LocalProvider(
                min_blocks=1,
                init_blocks=1,
                max_blocks=1,
                nodes_per_block=1,
                parallelism=0.5
            )
        )
    ]
)

parsl.load(config)


# @container_app(type="singularity", image="/home/darren/alcf/singularity/git/gsas2container/gsas2.img", data="/home/darren/alcf/singularity/work/data", cmd="/home/darren/alcf/singularity/git/singularity/builddir/singularity")
@python_app(executors=['local_htex'])
def PDF_Workflow(inputs=[]):

    def generate_charts(data, prefix, dest, xlabel, ylabel):
        import plotly.graph_objects as go
        import plotly

        import numpy as np

        x, y = np.loadtxt(data, unpack=True)

        layout = go.Layout(
            title=prefix,
            xaxis=dict(
                title=xlabel
            ),
            yaxis=dict(
                title=ylabel
            ))
        # Create traces
        fig = go.Figure(layout=layout, layout_xaxis_range=[0, 20])
        line = dict(color="#ffe476")
        scatter = go.Scatter(x=x, y=y,
                             mode='lines',
                             line=dict(color="#003865"),
                             name='lines')
        fig.add_trace(scatter)
        data = [scatter]
        div = plotly.offline.plot(data, include_plotlyjs=False, output_type='div')
        fig.write_html(dest + "/" + prefix + ".html")

    import numpy as np
    import matplotlib.pyplot as plt
    from matplotlib.axis import Axis
    import GSASIIscriptable as G2sc
    import sys
    sys.path.append("/home/darren/gsas")

    data = inputs[0]

    filename = data['filename']
    ceo2ImageFile = data['images']['CeO2']
    ceo2ControlFile = data['controls']['CeO2']
    capillaryImageFile = data['images']['Capillary']
    capillaryControlFile = data['controls']['Capillary']
    sulfurImageFile = data['images']['Sulfur']
    sulfurControlFile = data['controls']['Sulfur']

    # Create workflow GSAS2 project
    gpx = G2sc.G2Project(filename=filename)

    # Load tif images
    gpx.add_image(ceo2ImageFile)
    gpx.add_image(capillaryImageFile)
    gpx.add_image(sulfurImageFile)

    ceo2Image = gpx.images()[0]
    capillaryImage = gpx.images()[1]
    sulfurImage = gpx.images()[2]

    ceo2Image.loadControls(ceo2ControlFile)
    capillaryImage.loadControls(capillaryControlFile)
    sulfurImage.loadControls(sulfurControlFile)

    sulfurPWDRList = sulfurImage.Integrate()
    capillaryPWDRList = capillaryImage.Integrate()

    sulfurPWDRList[0].SaveProfile('pwdr_Sulfur')
    capillaryPWDRList[0].SaveProfile('pwdr_Capillary')

    sulfurPowerFile = data['powders']['Sulfur']

    print(sulfurPWDRList)

    pdf = gpx.add_PDF(sulfurPowerFile, 0)

    pdf.set_formula(['S', 1])
    pdf.data['PDF Controls']['Container']['Name'] = capillaryPWDRList[0].name
    pdf.data['PDF Controls']['Container']['Mult'] = -0.988
    pdf.data['PDF Controls']['Form Vol'] = 13.306
    pdf.data['PDF Controls']['Geometry'] = 'Cylinder'
    pdf.data['PDF Controls']['DetType'] = 'Area Detector'

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

    for i in range(5):
        if pdf.optimize():
            break

    pdf.calculate()

    pdf.export(data['export']['prefix'], 'I(Q), S(Q), F(Q), G(r)')

    gpx.save()

    x, y = np.loadtxt(data['export']['prefix'] + '.gr', unpack=True)

    plt.plot(x, y, label='Sulfur')
    fig = plt.figure(1)

    plt.title('Sulfur G(R)')
    plt.xlabel('x')
    plt.ylabel('y')

    axes = plt.gca()

    plt.xlim(data['limits']['xlim'])
    plt.ylim(data['limits']['ylim'])

    plt.savefig(data['outputfig'])
    os.makedirs(data['datadir'] + "/charts", exist_ok=True)
    generate_charts(data['export']['prefix'] + '.gr', os.path.basename(data['export']['prefix']) + "-gr", data['datadir'] + "/charts", "heat", "G(R)")

    x, y = np.loadtxt(data['export']['prefix'] + '.sq', unpack=True)

    plt.plot(x, y, label='Sulfur')
    fig = plt.figure(1)

    plt.title('Sulfur S(Q)')
    plt.xlabel('x')
    plt.ylabel('y')

    axes = plt.gca()

    plt.xlim(data['limits']['xlim'])
    plt.ylim(data['limits']['ylim'])

    plt.savefig(data['outputfig'])
    os.makedirs(data['datadir'] + "/charts", exist_ok=True)
    generate_charts(data['export']['prefix'] + '.sq', os.path.basename(data['export']['prefix']) + "-sq", data['datadir'] + "/charts", "heat", "S(Q)")

    return True


datadir1 = "/home/darren/alcf/singularity/LiquidSulfur/Experimental data/Sample 1 Heat - Tues"
datadir2 = "/home/darren/alcf/singularity/LiquidSulfur/Experimental data/Sample 2 Heat - Wed"
datadir3 = "/home/darren/alcf/singularity/LiquidSulfur/Experimental data/Sample 3 Cool - Thurs"
datadir4 = "/home/darren/alcf/singularity/LiquidSulfur/Experimental data/Sample 3 Heat -Wed"
datadir4 = "/home/darren/alcf/singularity/LiquidSulfur/Experimental data/Sample 3 Heat -Wed"

datadirs = [datadir1, datadir2, datadir3, datadir4]


pdfs = []
start = time.time()

for datadir in datadirs[:1]:
    tifs = glob.glob(datadir + "/*.tif")

    data = {
        'filename': datadir + '/sulfur-test.gpx',
        'outputfig': datadir + '/sulfur.png',
        'datadir': datadir,
        'export': {
            'prefix': datadir + '/Sulfur_At2_413K-00147'
        },
        'limits': {
            'xlim': [0.012914191411475429, 24.096733130017093],
            'ylim': [-1.937982691074275, 4.881099529628997]
        },
        'images': {
            'CeO2': datadir2 + '/CeO2_0p1s30f-00001.tif',
            'Capillary': datadir2 + '/Capillary_413K-00471.tif',
            'Sulfur': datadir + '/Sulfur_At2_413K-00147.tif'
        },
        'controls': {
            'CeO2': datadir2 + '/ceo2.imctrl',
            'Capillary': datadir2 + '/capillary.imctrl',
            'Sulfur': datadir2 + '/sulfur.imctrl'
        },
        'powders': {
            'Sulfur': datadir2 + '/pwdr_Sulfur.instprm'
        }
    }

    for tif in tifs[:1]:
        _tif = os.path.basename(tif)
        data['export']['prefix'] = datadir + '/' + _tif.split('.')[0]
        data['filename'] = datadir + '/' + _tif.split('.')[0] + '.gpx'
        data['outputfig'] = datadir + '/' + _tif.split('.')[0] + '.png'
        data['images']['Sulfur'] = datadir + '/' + _tif

        pdf = PDF_Workflow(inputs=[data])
        pdfs += [pdf]

[pdf.result() for pdf in pdfs]
end = time.time()
duration = str(datetime.timedelta(seconds=end - start))
print("DURATION:", duration)
