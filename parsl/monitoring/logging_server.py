import json
import logging
import time
import os

# Try to get rid of streamed loggers
root_logger = logging.getLogger()
root_logger.addHandler(logging.NullHandler())

import tornado.ioloop
import tornado.web
from parsl.monitoring.db_logger import get_db_logger

from parsl.monitoring.db_local import (create_workflows_table, create_task_status_table,
                                           create_task_table, create_task_resource_table)

try:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    import sqlalchemy as sa
except ImportError:
    pass

class MainHandler(tornado.web.RequestHandler):
    """ A handler for all requests/logs sent to the logging server."""
    def initialize(self, monitoring_config):
        """This function is called on every request which is not ideal but __init__ does not appear to work."""
        self.monitoring_config = monitoring_config

    def get(self):
        """Defines responses to get requests for the / ending. Not used by Parsl but could be."""
        self.write('Hello world - Parsl Logging Server')
        self.flush()
    
    def post(self):
        """
        Defines responses to post requests for the / ending. Receives logs from workers and main dfk in the body of the post request.
        Should be log=json.dumps(info). Then writes this info to the database using the database handler.
        This needs to be a quick function so that the server can accept other requests and may be a bottle neck for incoming logs.
        """
  
        arg = json.loads(self.get_body_argument('log'))
        info = {key: value for key, value in arg.items() if not key.startswith("__")}
        
        info['task_fail_history'] = str(info.get('task_fail_history', None))
        #info['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(arg.created))
        run_id = info['run_id']
        try:
            # if workflow or task has completed, update their entries with the time.
            if 'time_completed' in info.keys() and info['time_completed'] != 'None':
                self.application.loggers['workflows'].info(info) 
                return

            if 'task_time_returned' in info.keys() and info['task_time_returned'] != 'None':
                self.application.loggers['tasks'].info(info) 

            # if this is the first sight of the workflow, add it to the workflows table
            if info['run_id'] not in self.application.logged_workflows:
                self.application.loggers['workflows'].info(info)
                self.application.logged_workflows.add(info['run_id'])

            # if log has task counts, update the workflow entry in the workflows table
            if 'tasks_completed_count' in info.keys() or 'tasks_failed_count' in info.keys():
                self.application.loggers['workflows'].info(info)

            # check to make sure it is a task log and not just a workflow overview log
            if info.get('task_id', None) is not None:
                # if this is the first sight of the task in the workflow, add it to the workflow table
                if info['task_id'] not in self.application.logged_tasks:
                    if 'psutil_process_pid' in info.keys():
                        # this is the race condition that a resource log is before a status log so ignore this resource update
                        return

                    self.application.loggers['tasks'].info(info)
                    self.application.logged_tasks.add(info['task_id'])
                    
                if 'task_status' in info.keys():
                    self.application.loggers['task_status'].info(info)
                    return

                if 'psutil_process_pid' in info.keys():
                    self.application.loggers['task_resources'].info(info)
                    return
        except Exception as e:
            self.write(e)
            self.flush()

#         try:
#             self.application.logger.info('from tornado task ' + str(arg.get('task_id', 'NO TASK')), extra=arg)
#         except AttributeError:
#             self.application.logger = get_db_logger(logger_name='loggingserver', is_logging_server=True, monitoring_config=self.monitoring_config, db_engine=self.application.db_engine)
#             self.application.logger.info('from tornado task ' + str(arg.get('task_id', 'NO TASK')), extra=arg)

            
class MyApplication(tornado.web.Application):
    """ Inhierited tornado.web.Application - stowing some sqlalchemy session information in the application """
    def __init__(self, *args, **kwargs):
        """ setup the session to engine linkage in the initialization """
        self.monitoring_config = kwargs.pop('monitoring_config')
        self.db_engine = create_engine(self.monitoring_config.eng_link)
        self.metadata = sa.MetaData()
        self.logged_tasks = set()
        self.logged_workflows = set()
        self.create_loggers()
        super(MyApplication, self).__init__(*args, **kwargs)

    def create_database(self):
        """ this will create a database """
        create_workflows_table(self.metadata)
        create_task_status_table(self.metadata)
        create_task_table(self.metadata)
        create_task_resource_table(self.metadata)
        self.metadata.create_all(self.db_engine)
    
    def create_loggers(self):
        self.loggers = {}
        os.makedirs('monitoring_log', exist_ok=True)
        for log in ['workflows', 'tasks', 'task_status', 'task_resources']:
            logger = logging.getLogger(log)
            logger.setLevel(logging.DEBUG)
            fh = logging.FileHandler('monitoring_log/{}.log'.format(log))
            fh.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(message)s')
            logger.addHandler(fh)
            self.loggers[log] = logger
      
        
def run(monitoring_config):
    """ Set up the logging server according to configurations the user specified. This is the function launched as a separate process from the DFK in order to
    start logging. """
    # Assumtion that monitoring_config is not none because if it were this server should not have been started
    app = MyApplication(
                      [(r"/", MainHandler, dict(monitoring_config=monitoring_config))],
                      monitoring_config=monitoring_config,
    )
    #app.create_database()
    
#     app = tornado.web.Application([
#                 (r"/", MainHandler, dict(monitoring_config=monitoring_config))
#                 ])

    app.listen(monitoring_config.web_app_port)
    tornado.ioloop.IOLoop.current().start()
    
if __name__ == "__main__":
    from parsl.monitoring.db_logger import MonitoringConfig
    monitoring_config = MonitoringConfig(database_type='local_database',
                                         logger_name='parsl_db_logger',
                                         eng_link='sqlite:///parsl.db',
                                         web_app_host='http://localhost',
                                         web_app_port=88899,
                                         resource_loop_sleep_duration=1)

    run(monitoring_config)
