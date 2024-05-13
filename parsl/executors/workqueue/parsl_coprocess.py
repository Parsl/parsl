#! /usr/bin/env python3

import json
import os
import socket
import sys

# If enabled, coprocess will print to stdout
debug_mode = False

# Send a message on a binary I/O stream by sending the message length and then the (string) message.
def send_message(stream, data):
    size = len(data)
    size_msg = "{}\n".format(size)
    stream.write(size_msg)
    stream.write(data)

# Receive a standard message from a binary I/O stream by reading length and then returning the (string) message
def recv_message(stream):
    line = stream.readline()
    length = int(line)
    return stream.read(length)

# Decorator for remotely execution functions to package things as json.
def remote_execute(func):
    def remote_wrapper(event):
        kwargs = event["fn_kwargs"]
        args = event["fn_args"]
        try:
            response = {
                "Result": func(*args, **kwargs),
                "StatusCode": 200
            }
        except Exception as e:
            response = {
                "Result": str(e),
                "StatusCode": 500
            }
        return response
    return remote_wrapper

# Main loop of coprocess for executing network functions.
def main():
    # Listen on an arbitrary port to be reported to the worker.
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(('localhost', 0))
    except Exception as e:
        s.close()
        print(e, file=sys.stderr)
        sys.exit(1)

    # Inform the worker of name and port for later connection.
    config = {
        "name": name(),  # noqa: F821
        "port": s.getsockname()[1],
    }
    send_message(sys.stdout, json.dumps(config))
    sys.stdout.flush()

    # Remember original working directory b/c we change for each invocation.
    abs_working_dir = os.getcwd()

    # Create pipe for communication with child process
    rpipe, wpipe = os.pipe()
    rpipestream = os.fdopen(rpipe, "r")

    while True:
        s.listen()
        conn, addr = s.accept()
        connstream = conn.makefile("rw", encoding="utf-8")

        if debug_mode:
            print('Network function: connection from {}'.format(addr), file=sys.stderr)

        while True:
            # Read the invocation header from the worker
            line = connstream.readline()

            # If end of file, then break out and accept again
            if not line:
                break

            # Parse the invocation header.
            input_spec = line.split()
            function_name = input_spec[0]
            task_id = int(input_spec[1])
            event_size = int(input_spec[2])

            # then read the contents of the event itself
            event_str = connstream.read(event_size)
            event = json.loads(event_str)
            exec_method = event.get("remote_task_exec_method", None)

            try:
                # First move to target directory (is undone in finally block)
                os.chdir(os.path.join(abs_working_dir, f't.{task_id}'))

                # Then invoke function by desired method, resulting in
                # response containing the text representation of the result.

                if exec_method == "direct":
                    response = json.dumps(globals()[function_name](event))
                else:
                    p = os.fork()
                    if p == 0:
                        response = globals()[function_name](event)
                        wpipestream = os.fdopen(wpipe, "w")
                        send_message(wpipestream, json.dumps(response))
                        wpipestream.flush()
                        os._exit(0)
                    elif p < 0:
                        if debug_mode:
                            print(f'Network function: unable to fork to execute {function_name}', file=sys.stderr)
                        response = {
                            "Result": "unable to fork",
                            "StatusCode": 500
                        }
                        response = json.dumps(response)
                    else:
                        # Get response string from child process.
                        response = recv_message(rpipestream)
                        # Wait for child process to complete
                        os.waitpid(p, 0)

                # At this point, response is set to a value one way or the other

            except Exception as e:
                if debug_mode:
                    print("Network function encountered exception ", str(e), file=sys.stderr)
                response = {
                    'Result': f'network function encountered exception {e}',
                    'Status Code': 500
                }
                response = json.dumps(response)
            finally:
                # Restore the working directory, no matter how the function ended.
                os.chdir(abs_working_dir)

            # Send response string back to parent worker process.
            send_message(connstream, response)
            connstream.flush()

    return 0
def name():
    return 'parsl_coprocess'
@remote_execute
def run_parsl_task(a, b, c):
    import parsl.executors.workqueue.exec_parsl_function as epf
    try:
        (map_file, function_file, result_file) = (a, b, c)
        try:
            (namespace, function_code, result_name) = epf.load_function(map_file, function_file)
        except Exception:
            raise
        try:
            result = epf.execute_function(namespace, function_code, result_name)
        except Exception:
            raise
    except Exception:
        result = RemoteExceptionWrapper(*sys.exc_info())
    epf.dump_result_to_file(result_file, result)
    return None
if __name__ == "__main__":
    main()

