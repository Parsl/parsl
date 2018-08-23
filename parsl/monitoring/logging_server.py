import tornado.ioloop
import tornado.web
import json
from parsl.monitoring.db_logger import get_db_logger


class MainHandler(tornado.web.RequestHandler):
    def initialize(self, db_logger_config):
        self.db_logger_config = db_logger_config

    def get(self):
        self.write('Hello world')
        self.flush()

    def post(self):
        arg = json.loads(self.get_body_argument('log'))
        try:
            self.application.logger.info('from tornado task ' + str(arg.get('task_id', 'NO TASK')), extra=arg)
        except AttributeError as e:
            self.application.logger = get_db_logger(logger_name='loggingserver', is_logging_server=True, **self.db_logger_config)
            self.application.logger.info('from tornado task ' + str(arg.get('task_id', 'NO TASK')), extra=arg)


def run(db_logger_config):
    # Assumtion that db_logger_config is not none because if it were this server should not have been started
    app = tornado.web.Application([(r"/", MainHandler, dict(db_logger_config=db_logger_config))])
    app.listen(db_logger_config.get('web_app_port', 8899))
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    run()
