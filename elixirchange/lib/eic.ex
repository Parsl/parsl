defmodule EIC do
  use Application

  require Logger

  @impl true
  def start(_type, _args) do
    EIC.Supervisor.start_link([])
  end

  @impl true
  def stop(_) do
    Logger.info("In EIC.Supervisor stop")
  end
end

defmodule EIC.Supervisor do
  @moduledoc """
  This is the top level supervisor
  """

  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, :ok, opts)
  end

  @impl true
  def init(:ok) do
    {:ok, ctx} = :erlzmq.context()
    children = [
      %{id: EIC.TasksSubmitToInterchange, start: {EIC.TasksSubmitToInterchange, :start_link, [ctx]}},
      %{id: EIC.CommandChannel, start: {EIC.CommandChannel, :start_link, [ctx]}},
      %{
        id: EIC.TasksInterchangeToWorkers,
        start: {EIC.TasksInterchangeToWorkers, :start_link, [ctx]}
      },
      %{
        id: EIC.ResultsWorkersToInterchange,
        start: {EIC.ResultsWorkersToInterchange, :start_link, [ctx]}
      },
      %{
        id: EIC.ResultsInterchangeToSubmit,
        start: {EIC.ResultsInterchangeToSubmit, :start_link, [ctx]}
      },
      %{id: EIC.Matchmaker, start: {EIC.Matchmaker, :start_link, [ctx]}},

      # Getting the syntax right for the arguments to this was very fiddly...
      # maybe because I was paging back into Elixir syntax...
      %{id: EIC.TaskSupervisor,
        start: {DynamicSupervisor, :start_link, [[name: EIC.TaskSupervisor, strategy: :one_for_one, max_restarts: 0]]}
      },
      %{id: EIC.TaskRegistry,
       start: {Registry, :start_link, [[name: EIC.TaskRegistry, keys: :unique]]}
      }
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 0)
  end
end

# TODO: can I abstract the various ZMQ modules as some kind of self-defined behaviour?
defmodule EIC.TasksSubmitToInterchange do
  @moduledoc """
  This module handles tasks coming from the submit side into the interchange
  over ZMQ.
  """
  require Logger

  def start_link(ctx) do
    Task.start_link(EIC.TasksSubmitToInterchange, :body, [ctx])
  end

  def body(ctx) do
    Logger.info("Starting tasks submit to interchange ZMQ handler")
    {:ok, socket} = :erlzmq.socket(ctx, :dealer)
    :ok = :erlzmq.connect(socket, "tcp://127.0.0.1:9000")
    loop(socket)
  end

  def loop(socket) do
    Logger.debug("Invoking task receive")
    {:ok, msg} = :erlzmq.recv(socket)
    # msg is a pickled dict with keys: task_id and buffer

    # This is always a new task, because this channel doesn't convey
    # any other message types - so unpickling doesn't have to happen
    # before dispatch into a new ParslTask process
    Logger.debug(["Received ", inspect(msg)])

    Logger.debug("Starting new ParslTask process")
    {:ok, pid} = DynamicSupervisor.start_child(EIC.TaskSupervisor, {EIC.ParslTask, msg})
    Logger.debug(["Started new ParslTask process, pid ", inspect(pid)])
    # this pid should get registered against the contained task ID... eventually... but not here, because we have to do an (expensive?) unpickle?

    loop(socket)
  end
end

defmodule EIC.ResultsWorkersToInterchange do
  @moduledoc """
  This module handles results coming from workers back into the interchange
  over ZMQ.
  """

  require Logger

  def start_link(ctx) do
    Task.start_link(EIC.ResultsWorkersToInterchange, :body, [ctx])
  end

  def body(ctx) do
    {:ok, socket} = :erlzmq.socket(ctx, :router)
    :ok = :erlzmq.bind(socket, "tcp://127.0.0.1:9004")
    loop(socket)
  end

  def loop(socket) do
    Logger.debug("Invoking results receive")
    {:ok, msgs} = :erlzmq.recv_multipart(socket)
    # this parts vec will contain first a manager ID, and then an arbitrary number of pickled result-like parts from that manager.
    Logger.debug(["Got this results multipart message:", inspect(msgs)])

    # unpack the results message here - if its expensive, what would it be like to send each part into its own task
    # that routed the result onwards?

    [manager_id | results] = msgs

    # I guess the protocol implicitly assumes that results come from the manager that we sent the task to...
    # but what happens if not? does everything still behave right?
    # If we don't care, then we can drop the manager ID from the message sequence and move to a less
    # complicated message protocol here, that does not need to be a DEALER+multipart message...
    # and if we do care... how does the Python implementation deal with a different manager ID?
    # For example, in insecure mode (and other such badnesses), perhaps we'd get a task from a manager
    # we don't even know about? In which case, a bunch of code possibly might break because it makes
    # the assumption that there will be some appropriate state for that particular manager...
    # TODO: protocol clarification: open an issue? write a test?

    Logger.info(["Results messages are from manager id: ", inspect(manager_id)])
    Logger.info(["There are ", inspect(length(results)), " results in this multipart message"])

    # each element in results is a pickled dict, with a "type" key that can be
    # result, heartbeat or monitoring.

    Enum.each(results, fn msg ->
      Logger.debug(["msg to unpickle: ", inspect(msg)])
      result_dict = :pickle.pickle_to_term(msg)
      result_type = :dict.fetch({:pickle_unicode, "type"}, result_dict)

      Logger.info(["result message type is: ", inspect(result_type)])

      # now we can dispatch on result_type...
      case result_type do
          {:pickle_unicode, "result"} -> deliver_result(result_dict, manager_id)
          {:pickle_unicode, "heartbeat"} -> Logger.info("Result-channel heartbeat, no need for implementation - see #3464")
          x -> raise "Unknown result message type"
      end

    end)

    loop(socket)
  end

  def deliver_result(result_dict, manager_id) do
    task_id = :dict.fetch({:pickle_unicode, "task_id"}, result_dict)

    # TODO: task_id might be -1 here...
    # which is used when sending back a pool-wide exception at startup
    # (specifically VersionMismatch) rather than a task-specific result.
    # There's no handling for that here. And no testing for it in the
    # test suite, I think... but it should go in the protocol documentation...
    # TODO: this -1 could turn into a different typed result, in the same
    # way as heartbeats are - rather than overloading "result" with magic
    # values. TODO: open an issue, #NNNN

    Logger.info(["Delivering result to ParslTask id ", inspect(task_id)])
    [{task_process, :whatever}] = Registry.lookup(EIC.TaskRegistry, task_id)
    GenServer.cast(task_process, {:result, result_dict, manager_id})
  end

end

defmodule EIC.ResultsInterchangeToSubmit do
  require Logger

  def start_link(ctx) do
    {:ok, pid} = Task.start_link(EIC.ResultsInterchangeToSubmit, :body, [ctx])
    Process.register(pid, EIC.ResultsInterchangeToSubmit)
    {:ok, pid}
  end

  def body(ctx) do
    Logger.info("Starting results interchange to submit handler")
    {:ok, socket} = :erlzmq.socket(ctx, :dealer)
    :ok = :erlzmq.connect(socket, "tcp://127.0.0.1:9001")
    loop(socket)
  end

  def loop(socket) do
    receive do
      m -> Logger.debug("sending result message to submit side")
           :erlzmq.send(socket, m)
    end
    loop(socket)
  end
end

defmodule EIC.CommandChannel do

  require Logger

  def start_link(ctx) do
    Task.start_link(EIC.CommandChannel, :body, [ctx])
  end

  def body(ctx) do
    Logger.info("CommandChannel: Starting command channel ZMQ handler")
    {:ok, socket} = :erlzmq.socket(ctx, :rep)
    :ok = :erlzmq.connect(socket, "tcp://127.0.0.1:9002")
    loop(socket)
  end

  def loop(socket) do
    Logger.debug("CommandChannel: blocking recv on zmq command channel")
    {:ok, msg} = :erlzmq.recv(socket)
    # msg is a pickled object that varies depending on the command
    # IO.inspect(msg)
    {:pickle_unicode, command} = :pickle.pickle_to_term(msg)
    Logger.debug(["Received command channel command: ", inspect(command)])

    # now dispatch this command using case matching... TODO

    response = handle_command(command)

    pickled_response = :pickle.term_to_pickle(response)

    Logger.debug(["pickled response: ", inspect(pickled_response)])

    :erlzmq.send(socket, pickled_response)

    loop(socket)
  end

  def handle_command("WORKER_PORTS") do
    # TODO: this can be dynamic... (since #3461)
    {9003, 9004}
  end

  def handle_command("CONNECTED_BLOCKS") do
    # TODO: talk to some process that will keep track of a set of seen blocks
    []
    # as supplied by manager registration messages
  end

  def handle_command("MANAGERS") do
    # TODO: scaling in won't work right for this... but that hasn't caused any
    # test failures so far...
    []
  end

  def handle_command(_bad) do
    # TODO: what should happen when we receive a bad command, protocol-wise?
    # The rusterchange, at this point, dies. But perhaps we should be sending
    # back eg. None and then continuing? Its probably not right to be dying
    # without resetting the command channel to a known good state? because of
    # the two-state setup of ZMQ REQ/REP sockets.
    raise "Unknown command on command channel"
  end
end

defmodule EIC.TasksInterchangeToWorkers do
  @moduledoc """
  This handles messages on the interchange to workers channel, which is
  actually bi-directional: registrations and heartbeats will be received
  in the reverse direction.

  Any process can send a message on this pipe by sending a message to the
  singleton process here. That message will be forwarded on through the
  zmq connection.

  TODO: there's a module that lets you poll a socket (and hence a ZMQ connection?)
  into an erlang message which would allow this kind of message driven stuff
  perhaps? It might be structured a bit differently though, with a zmq gateway
  module and then a module for the next level up in the protocol?
  https://github.com/msantos/inert
  I'm a little unclear about what can be polled in different OS threads here, but
  hopefully the poll can happen anywhere as long as no reading happens? the poll
  is just a chance for the zmq-level poll to happen?
  WRONG?: Because there's no "poll zmq inbound and process mailbox" facility in
  elixir, this is going to sit in a loop alternating between polling those
  two things separately. That's pretty horrible. Another erlang/zmq
  implementations have the ability to gateway incoming zmq directly into
  process mailbox, but is pretty out of date, and that functionality
  was removed in the erlzmq_dnif fork of that code. It would let this process
  be much more message driven, I think, if it existed.

  TODO: this cannot restart properly: when it is restarted by its supervisor,
  the bound socket from the previous process is left open and so it fails to
  perform the bind. should we fail completely and abandon the workflow?

  TODO: should this launch a separate process for each registered manager, to track
  things like heartbeats?

  """

  require Logger

  def start_link(ctx) do
    {:ok, pid} = Task.start_link(EIC.TasksInterchangeToWorkers, :body, [ctx])
    Process.register(pid, EIC.TasksInterchangeToWorkers)
    {:ok, pid}
  end

  def body(ctx) do
    Logger.info("TasksInterchangeToWorkers: in body")
    {:ok, socket_to_workers} = :erlzmq.socket(ctx, :router)
    :ok = :erlzmq.bind(socket_to_workers, "tcp://127.0.0.1:9003")

    # TODO: this timeout should be 0 and event driven with "inert"
    :ok = :erlzmq.setsockopt(socket_to_workers, :rcvtimeo, 1000)

    loop(socket_to_workers)
  end

  def loop(socket) do
    # IO.puts("TasksInterchangeToWorkers: recv poll")

    # TODO: this timeout polling mode is going to give a pretty bad
    # rate limit on throughput through the interchange... and pretty
    # ugly to do more fancy stuff like while loops that also try to
    # be fair on each direction. The active gatewaying stuff mentioned
    # above would be nicer... or being able to do a zmq poll on two
    # sockets and use inproc zmq sockets, which I started prototyping
    # then discovered you can't do zmq poll on multiple sockets in this
    # zmq implementation.

    # TODO: this can now be: {:error, :eagain}
    # when we hit timeout, where we would then loop around again
    case :erlzmq.recv_multipart(socket) do
      {:ok, [source, msg]} -> 
        Logger.debug(inspect(msg))
        decoded_msg = JSON.decode!(msg)
        Logger.debug(inspect(decoded_msg))
        handle_message_from_worker(source, decoded_msg)
      {:error, :eagain} ->
        Logger.debug("timeout no-op / recv from socket")
    end

    receive do
      m -> Logger.debug("TasksInterchangeToWorkers: sending a multipart message to workers")
           :erlzmq.send_multipart(socket, m)
    after 
      # TODO: there should not need to be a timeout here - everything should be driven by
      # erlang messages
      1000 -> Logger.debug("timeout no-op / send to socket")
    end


    loop(socket)
  end

  def handle_message_from_worker(source, %{"type" => "registration"} = msg) do
    # %{
    #  "block_id" => "0",
    # "cpu_count" => 4,
    # "dir" => "/home/benc/parsl/src/parsl",
    # "hostname" => "parsl-dev-3-12-9270",
    # "max_capacity" => 8,
    # "os" => "Linux",
    # "parsl_v" => "1.3.0-dev",
    # "prefetch_capacity" => 0,
    # "python_v" => "3.12.2",
    # "total_memory" => 16467460096,
    # "type" => "registration",
    # "uid" => "2ad23417220a",
    # "worker_count" => 8
    # }

   GenServer.cast(:matchmaker, {:new_manager, msg})
  end

  def handle_message_from_worker(source, %{"type" => "heartbeat"} = msg) do
    Logger.error("NOTIMPL: task side heartbeat... will probably cause worker pools to time out... and also will cause hangs when a worker pool dies")
  end

  # TODO: there can be heartbeat here, but I think the test suite might not be
  # testing it: it will only get sent (I think) if no tasks have been received
  # which might be a rare occurence in the task-heavy test environment? maybe
  # there should be an explicit test?
  # I encountered it in the test suite when not processing results in elixirchange
  # and so not sending any more tasks, and so the process worker pool eventually
  # decides to send a heartbeat on this channel because of the silence, I guess?
  def handle_message_from_worker(source, %{"type" => t} = msg) do
    raise "Unsupported message type: #{t}"
  end

  def handle_message_from_worker(source, msg) do
    raise "Unsupported message without type tag: #{msg}"
  end
end

defmodule EIC.Matchmaker do
  use GenServer
  require Logger

  def start_link(ctx) do
    Logger.info("Matchmaker: start_link")
    GenServer.start_link(EIC.Matchmaker, [ctx], name: :matchmaker)
  end

  @impl true
  def init([_ctx]) do
    Logger.info("Matchmaker: initializing")

    {:ok, %{:tasks => [], :managers => []}}
  end

  @impl true
  def handle_cast({:new_task, task_dict}, state) do
    Logger.debug("Matchmaker: received a task")

    # TODO: better syntax for updating individual entries in map rather than
    # listing them all copy style?
    new_state = %{:tasks => [task_dict | state[:tasks]], :managers => state[:managers]}

    new_state2 = matchmake(new_state)
    {:noreply, new_state2}
  end

  def handle_cast({:new_manager, registration_msg}, state) do
    new_state = %{:tasks => state[:tasks], :managers => [registration_msg | state[:managers]]}
    Logger.debug(["Matchmaker: new state:", inspect(new_state)])
    new_state2 = matchmake(new_state)
    {:noreply, new_state2}
  end

  def handle_cast({:return_worker, manager_id}, state) do
    Logger.debug(["Matchmaker: return worker to manager ", inspect(manager_id)])
    %{tasks: ts, managers: ms} = state
    # TODO: implement me...
    #        ... pull out the relevant manager
    #        ... increase its capacity
    #        ... and put it at the head of the manager list

    # assumption that there is exactly one matching manager, m
    # which is untrue in the case of unknown managers or other bugs
    {[m], m_rest} = Enum.split_with(ms, fn m_ -> m_["uid"] == manager_id end)

    c = m["max_capacity"]
    new_capacity = c + 1
    modified_m = Map.put(m, "max_capacity", new_capacity)

    new_state = %{:tasks => ts, :managers => [modified_m | m_rest]}
    new_state2 = matchmake(new_state)
    {:noreply, new_state2}
  end

  def handle_cast(rest, state) do
    Logger.error(["Matchmaker: ERROR: unmatched handle_cast: ", inspect(rest)])
    raise "Unhandled Matchmaker cast"
  end

  @impl true
  def handle_call(_what, _from, state) do
    Logger.error("Matchmaker: ERROR: handle call")
    raise "Unhandled Matchmaker call"
  end

  # TODO: manager should be guarded by available capacity and new state should
  # modify that capacity, rather than forgetting the whole manager...
  # and maybe that means this can't be implemented in function guard style?
  def matchmake(%{:tasks => [task_dict | t_rest], :managers => [m | m_rest]} = state) do
    Logger.info("Made a match")
    # TODO: send this off to execute
    # also, record the pairing somehow so that we do appropriate behaviour on
    # task result or manager failure.

    # getting that into the interchange->workers ZMQ socket seems pretty awkward
    # but maybe I can do it with an inproc: zmq message and a poller, so that
    # we use ZMQ messaging from inside matchmake instead of erlang messaging?

    # TODO: can't reuse pickled form, I think? maybe I wrote about it in the
    # rusterchange? - this could become an issue for interchange protocol in the
    # parsl issue tracking, including a question about if its worth it (double
    # memory usage per queued task vs re-pickle speed, but the re-pickling is quite
    # a simple (2 element) dict)

    # the third element of this list of parts should itself be a list (of tasks)
    # rather than a single pickled task... (TODO: is that protocol difference
    # needed? should message parts always be a single task, with zmq-level part
    # separation instead of python level lists? There's perhaps some pickle-level
    # caching that goes away if so, when multiple tasks are sent to a single
    # manager at once... but at the same time, it forces more pickle-level
    # deserialization then re-serialization rather than being able to skip that?
    # and maybe keeping things binary is a good thing? TODO: look at that
    # performance-sensitive path, maybe open an issue?

    # this pickling here is pickling byte sequences using Python2 pickle formats
    # which don't unpickle correctly - BINSTRING not BINBYTES - so probably need
    # to fiddle with the elixir-side pickle library some more.

    # TODO: should this pickle happen in the task process? In the Python interchange,
    # we might be sending multiple tasks here in this list, but in this impl,
    # we're only ever sending one... concurrency unlock potential in pickling
    # in task-specific code?

    # TODO: case for when we don't have a live manager... so we need to defer
    # (until the matchmaker gets tickled again by a worker being released)

    c = m["max_capacity"]
    if c < 1 do
      Logger.debug("Head manager has no capacity... no match to make")  

      # TODO: is there a syntax to avoid reconstructing these args?
      %{:tasks => [task_dict | t_rest], :managers => [m | m_rest]}
    else

      Logger.debug(["Selected manager has max_capacity: ", inspect(c)])
      pickled_list_of_tasks = :pickle.term_to_pickle([task_dict])
      parts = [m["uid"], <<>>, pickled_list_of_tasks] 

      send(EIC.TasksInterchangeToWorkers, parts)

      new_capacity = c - 1
      modified_m = Map.put(m, "max_capacity", new_capacity)

      new_managers_state = if new_capacity < 1 do
        # move this manager to the end of the list, so that other managers
        # potentially with task slots, are earlier in the list (specifically,
        # if any manager has an available task slot, then the head of the list must
        # have an available task slot - would be interesting to assert that
        # somewhere? eg. at the start of each handle_cast?
        new_managers_state = m_rest ++ [modified_m]
      else
        # if we still have capactity, keep this manager at the head of the
        # list - because we know it is a manager with capacity, so that is a
        # suitable list head... 
        new_managers_state = [modified_m | m_rest]
      end

      %{:tasks => t_rest, :managers => new_managers_state}
    end
  end

  def matchmake(state) do
    Logger.debug("No match made between any manager or task")
    state
  end
end

defmodule EIC.ParslTask do
  use GenServer
  # TODO: the supervisor for this should not relaunch task on failure, but fail fast
  # because I don't want in-interchange task retry style behaviour.

  # I guess I'll implement this with GenServer, dealing mostly with
  # handle_cast rather than handle_call?
  require Logger

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  def init(pickled_message) do
    # This call is synchronous, so don't do anything heavy... otherwise will
    # block the message loop that started this ParslTask and stop it starting
    # others, which potentially will lose some concurrency?

    Logger.info(["ParslTask starting with pickled task message ", inspect(pickled_message)])
    initial_state = [pkl: pickled_message]
    # TODO: maybe don't want to keep pkl around after we've submitted the task?

    # by the time we're here, we have a pickled task message. we don't know
    # the task ID yet, so there's no way to lookup a result message and get
    # it back to this ParslTask...

    # carry on the initialization inside the process...
    GenServer.cast(self(), :init_in_process)
    {:ok, initial_state}
  end

  def handle_cast(:init_in_process, state) do
    Logger.info("Task deferred initialization")

    Logger.debug("unpickling")
    {:ok, pkl} = Keyword.fetch(state, :pkl)
    task_dict = :pickle.pickle_to_term(pkl)
    Logger.debug(["unpickled task_dict is:", inspect(task_dict)])
    # this inspect representation of task_dict is pretty hard to read: there is a lot
    # of erlang-level noise in there from the erlang :dict module, rather than it
    # looking like a "simple" translation...
    # so here's a little bit of pretty-logging:
    Logger.debug(["task_dict has these keys: ", inspect(:dict.fetch_keys(task_dict))])

    task_id = :dict.fetch({:pickle_unicode, "task_id"}, task_dict)
    Logger.info(["Task ID is: ", inspect(task_id)])

    {:ok, _owner} = Registry.register(EIC.TaskRegistry, task_id, :whatever)

    GenServer.cast(:matchmaker, {:new_task, task_dict})

    # new state does not have pkl in it, because we don't need it any more
    {:noreply, []}
  end

  def handle_cast({:result, result_dict, manager_id}, []) do
    Logger.warn("in ParslTask result handler")
    pickled_result = :pickle.term_to_pickle(result_dict)
    send(EIC.ResultsInterchangeToSubmit, pickled_result)
   
    # we can assume (see TODO issue #NNNN) that the manager_id here is the
    # manger that should be released - although there's no test for it...

    GenServer.cast(:matchmaker, {:return_worker, manager_id})

    {:noreply, []}
  end

end
