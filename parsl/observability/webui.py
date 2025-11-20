import sys

from http.server import HTTPServer, SimpleHTTPRequestHandler

from parsl.observability.utils import get_all_values, widen_by_implication
from parsl.observability.graph_utils import plot_task_lines

import parsl.observability.rerun_monitoring_db as rr

class RequestHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/p/"):
            self.path = self.path[3:]
            return super().do_GET()

        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(bytes('<html><h1>parsl observability web ui</h1>', 'utf-8'))
        self.wfile.write(bytes(f'<p>There are {len(logs)} log records imported.</p>', 'utf-8'))

        # count workflows: that means, count parsl_dfk values

        dfks = get_all_values(logs, 'parsl_dfk')
        self.wfile.write(bytes(f'<p><code>parsl_dfk</code>: These workflows are known: {dfks}</p>', 'utf-8'))

        # TODO: now draw timelines for DFKs: two kinds of graphs:
        #   number of DFKs alive at one time, and line graph
        # number of DFKs at once is maybe uninteresting, but it
        # starts the habit of graph vs source orthogonality. These
        # graphs exist for tasks in blog plot file.

        
        self.wfile.write(bytes('<h2>Timelines for tasks (potentially narrowed by above)</h2>', 'utf-8'))
        # timelines for all tasks (in all DFKs until I do any DFK selection)

        import matplotlib.pyplot as plt
       
        dpi = 100
        plt.figure(figsize=(600 / dpi, 400 / dpi))

        plot_task_lines(logs, plt)

        plt.savefig("a.png", dpi=dpi)
        plt.close()
 
        self.wfile.write(bytes('<p><img src="/p/a.png"></p>', 'utf-8'))

        self.wfile.write(bytes('</html>', 'utf-8'))

        print(self.path)


if __name__ == "__main__":
  print("starting Parsl observability HTTP server")

  logs=rr.rerun("runinfo/monitoring.db")
  widen_by_implication(logs, ('parsl_dfk', 'parsl_task_id'), 'parsl_app_name')

  s = HTTPServer( ("127.0.0.1", int(sys.argv[1])), RequestHandler)
  s.serve_forever()
