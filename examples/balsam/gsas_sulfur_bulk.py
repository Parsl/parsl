import glob
import os
import time
import datetime

datadir = "/home/darren/alcf/singularity/LiquidSulfur/Experimental data/Sample 2 Heat - Wed"


def generate_charts(data, prefix, dest):
    import plotly.graph_objects as go
    import plotly

    import numpy as np

    x, y = np.loadtxt(data, unpack=True)

    # Create traces
    fig = go.Figure(layout_xaxis_range=[0, 20])
    line = dict(color="#ffe476")
    scatter = go.Scatter(x=x, y=y,
                        mode='lines',
                        line=dict(color="#003865"),
                        name='lines')
    fig.add_trace(scatter)
    data = [scatter]
    div = plotly.offline.plot(data, include_plotlyjs=False, output_type='div')
    print(div)
    fig.write_html(dest+"/"+prefix+".html")


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

    ceo2Image.loadControls(datadir+'/ceo2.imctrl')
    capillaryImage.loadControls(datadir+'/capillary.imctrl')
    sulfurImage.loadControls(datadir+'/sulfur.imctrl')

    sulfurPWDRList = sulfurImage.Integrate()
    capillaryPWDRList = capillaryImage.Integrate()

    sulfurPWDRList[0].SaveProfile('pwdr_Sulfur')
    capillaryPWDRList[0].SaveProfile('pwdr_Capillary')

    print(sulfurPWDRList)

    pdf = gpx.add_PDF(datadir+'/pwdr_Sulfur.instprm', 0)

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

    generate_charts(data['export']['prefix']+'.gr', os.path.basename(data['export']['prefix']), data['datadir']+"/charts")

    return data


data = {
    'filename': datadir+'/sulfur-test.gpx',
    'outputfig': datadir+'/sulfur.png',
    'datadir': datadir,
    'export': {
        'prefix': datadir+'/Sulfur_At2_413K-00147'
    },
    'limits': {
        'xlim': [0.012914191411475429, 24.096733130017093],
        'ylim': [-1.937982691074275, 4.881099529628997]
    },
    'images': {
        'CeO2': datadir+'/CeO2_0p1s30f-00001.tif',
        'Capillary': datadir+'/Capillary_413K-00471.tif',
        'Sulfur': datadir+'/Sulfur_At2_413K-00147.tif'
    },
    'controls': {
        'CeO2': datadir+'/ceo2.imctrl',
        'Capillary': datadir+'/capillary.imctrl',
        'Sulfur': datadir+'/sulfur.imctrl'
    }
}

tifs = glob.glob(datadir+"/*.tif")

start = time.time()

count = 1
for tif in tifs:
    _tif = os.path.basename(tif)
    data['export']['prefix'] = datadir+'/'+_tif.split('.')[0]
    data['filename'] = datadir+'/projects/'+_tif.split('.')[0]+'.gpx'
    data['outputfig'] = datadir+'/images/'+_tif.split('.')[0]+'.png'
    data['images']['Sulfur'] = datadir+'/'+_tif

    PDF_Workflow(inputs=[data])
    print("COMPLETED {}".format(count))
    count += 1

end = time.time()
duration = str(datetime.timedelta(seconds = end-start))
print("DURATION:",duration)
