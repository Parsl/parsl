import tornado.ioloop
import tornado.web
import json
from parsl.monitoring.db_logger import get_db_logger


class MainHandler(tornado.web.RequestHandler):
    def initialize(self, db_logger_config=None):
        # self.logger = get_db_logger(enable_es_logging=False) if db_logger_config is None else get_db_logger(**db_logger_config)
        self.logger = get_db_logger(logger_name='tornadoServer', enable_local_db_logging=True)

    def get(self):
        self.write('Hello world')
        self.flush()

    def post(self):
        arg = self.get_body_argument('log')
        self.logger.info('from tornado', extra=json.loads(arg))
        self.write('0')
        self.flush()


def run(addr=8899, db_logger_config=None):
    app = tornado.web.Application([(r"/", MainHandler, dict(db_logger_config=db_logger_config))])
    app.listen(addr)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    run()
