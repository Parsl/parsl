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
      %{id: EIC.TaskQueue, start: {EIC.TaskQueue, :start_link, [ctx]}},

      # Getting the syntax right for the arguments to this was very fiddly...
      # maybe because I was paging back into Elixir syntax...
      %{id: EIC.TaskSupervisor,
        start: {DynamicSupervisor, :start_link, [[name: EIC.TaskSupervisor, strategy: :one_for_one]]}
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

    Logger.warn("NOTIMPL: not doing anything with result message")
    # TODO: if there are per-task processes, send this message onwards to
    # the relevant task process... which I haven't implemented...
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
    # TOOD: this can be dynamic... (since #3461)
    {9003, 9004}
  end

  def handle_command("CONNECTED_BLOCKS") do
    # TODO: talk to some process that will keep track of a set of seen blocks
    []
    # as supplied by manager registration messages
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

  # TODO: there can be heartbeat here, but I think the test suite might not be
  # testing it: it will only get sent (I think) if no tasks have been received
  # which might be a rare occurence in the task-heavy test environment? maybe
  # there should be an explicit test?
  # I encountered it in the test suite when not processing results in elixirchange
  # and so not sending any more tasks, and so the process worker pool eventually
  # decides to send a heartbeat on this channel because of the silence, I guess?
  def handle_message_from_worker(source, msg) do
    raise "Unsupported message"
  end
end

defmodule EIC.TaskQueue do
  use GenServer
  require Logger

  def start_link(ctx) do
    Logger.info("TaskQueue: start_link")
    GenServer.start_link(EIC.TaskQueue, [ctx], name: :matchmaker)
  end

  @impl true
  def init([_ctx]) do
    Logger.info("TaskQueue: initializing")

    {:ok, %{:tasks => [], :managers => []}}
  end

  @impl true
  def handle_cast({:new_task, task_dict, pickled_msg}, state) do
    Logger.debug("TaskQueue: received a task")

    # TODO: better syntax for updating individual entries in map rather than
    # listing them all copy style?
    new_state = %{:tasks => [{task_dict, pickled_msg} | state[:tasks]], :managers => state[:managers]}

    new_state2 = matchmake(new_state)
    {:noreply, new_state}
  end

  def handle_cast({:new_manager, registration_msg}, state) do
    new_state = %{:tasks => state[:tasks], :managers => [registration_msg | state[:managers]]}
    Logger.debug(["TaskQueue: new state:", inspect(new_state)])
    new_state2 = matchmake(new_state)
    {:noreply, new_state2}
  end

  def handle_cast(rest, state) do
    Logger.error(["TaskQueue: ERROR: leftover handle_cast", inspect(rest)])
    raise "Unhandled TaskQueue cast"
  end

  @impl true
  def handle_call(_what, _from, state) do
    Logger.error("TaskQueue: ERROR: handle call")
    raise "Unhandled TaskQueue call"
  end

  # TODO: manager should be guarded by available capacity and new state should
  # modify that capacity, rather than forgetting the whole manager...
  # and maybe that means this can't be implemented in function guard style?
  def matchmake(%{:tasks => [t | t_rest], :managers => [m | m_rest]} = state) do
    Logger.info("Made a match")
    {task_dict, _pickled} = t
    # TODO: send this off to execute
    # also, record the pairing somehow so that we do appropriate behaviour on
    # task result or manager failure.

    # getting that into the interchange->workers ZMQ socket seems pretty awkward
    # but maybe I can do it with an inproc: zmq message and a poller, so that
    # we use ZMQ messaging from inside matchmake instead of erlang messaging?

    # TODO: can't reuse pickled form, I think? maybe I wrote about it in the
    # rusterchange?
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
    pickled_list_of_tasks = :pickle.term_to_pickle([task_dict])  # TODO: WRONG?
    parts = [m["uid"], <<>>, pickled_list_of_tasks] 

    send(EIC.TasksInterchangeToWorkers, parts)

    %{:tasks => t_rest, :managers => m_rest}
  end

  def matchmake(state) do
    Logger.debug("No match made between any manager or task")
    state
  end
end

defmodule EIC.ParslTask do
  use GenServer
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

    # TODO: figure out the task ID here and register it

    GenServer.cast(:matchmaker, {:new_task, task_dict, :unused})

    # new state does not have pkl in it, because we don't need it any more
    {:noreply, []}
  end

end