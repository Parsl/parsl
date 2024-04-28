defmodule EIC do
  use Application

  @impl true
  def start(_type, _args) do
    EIC.Supervisor.start_link([])
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
    children = [
      %{id: EIC.TasksSubmitToInterchange, start: {EIC.TasksSubmitToInterchange, :start_link, []}},
      %{id: EIC.CommandChannel, start: {EIC.CommandChannel, :start_link, []}},
      %{
        id: EIC.TasksInterchangeToWorkers,
        start: {EIC.TasksInterchangeToWorkers, :start_link, []}
      },
      %{id: EIC.TaskQueue, start: {EIC.TaskQueue, :start_link, []}}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

# TODO: can I abstract the various ZMQ modules as some kind of self-defined behaviour?
defmodule EIC.TasksSubmitToInterchange do
  @moduledoc """
  This module handles tasks coming from the submit side into the interchange
  over ZMQ.
  """

  def start_link() do
    Task.start_link(EIC.TasksSubmitToInterchange, :body, [])
  end

  def body() do
    IO.puts("Starting tasks submit to interchange ZMQ handler")
    {:ok, ctx} = :erlzmq.context()
    {:ok, socket} = :erlzmq.socket(ctx, :dealer)
    :ok = :erlzmq.connect(socket, "tcp://127.0.0.1:9000")
    loop(socket)
  end

  def loop(socket) do
    IO.puts("Invoking task receive")
    {:ok, msg} = :erlzmq.recv(socket)
    # msg is a pickled dict with keys: task_id and buffer
    IO.inspect(msg)
    {task_dict, ""} = Unpickler.load!(msg)
    IO.inspect(task_dict)

    IO.puts("casting task to task queue")
    GenServer.cast(:task_queue, {:new_task, task_dict})

    loop(socket)
  end
end

defmodule EIC.CommandChannel do
  def start_link() do
    Task.start_link(EIC.CommandChannel, :body, [])
  end

  def body() do
    IO.puts("CommandChannel: Starting command channel ZMQ handler")
    {:ok, ctx} = :erlzmq.context()
    {:ok, socket} = :erlzmq.socket(ctx, :rep)
    :ok = :erlzmq.connect(socket, "tcp://127.0.0.1:9002")
    loop(socket)
  end

  def loop(socket) do
    IO.puts("CommandChannel: recv")
    {:ok, msg} = :erlzmq.recv(socket)
    # msg is a pickled object that varies depending on the command
    IO.inspect(msg)
    {command, ""} = Unpickler.load!(msg)
    IO.inspect(command)

    # now dispatch this command using case matching... TODO

    response = handle_command(command)

    pickled_response = :pickle.term_to_pickle(response)

    IO.inspect(pickled_response)

    :erlzmq.send(socket, pickled_response)

    loop(socket)
  end

  def handle_command("CONNECTED_BLOCKS") do
    # TODO: talk to some process that will keep track of a set of seen blocks
    []
    # as supplied by manager registration messages
  end

  def handle_command(_bad) do
    raise "Unhandled command"
  end
end

defmodule EIC.TasksInterchangeToWorkers do
  # TODO: this cannot restart properly: when it is restarted by its supervisor,
  # the bound socket from the previous process is left open and so it fails to
  # perform the bind.

  # should this launch a separate process for each registered manager, to track
  # things like heartbeats?

  def start_link() do
    Task.start_link(EIC.TasksInterchangeToWorkers, :body, [])
  end

  def body() do
    IO.puts("TasksInterchangeToWorkers: in body")
    {:ok, ctx} = :erlzmq.context()
    {:ok, socket} = :erlzmq.socket(ctx, :router)
    :ok = :erlzmq.bind(socket, "tcp://127.0.0.1:9003")
    loop(socket)
  end

  def loop(socket) do
    IO.puts("TaskInterchangeToWorkers: recv")
    {:ok, [source, msg]} = :erlzmq.recv_multipart(socket)
    IO.inspect(msg)

    decoded_msg = JSON.decode!(msg)

    IO.inspect(decoded_msg)

    handle_message(decoded_msg)
    loop(socket)
  end

  def handle_message(%{"type" => "registration"} = msg) do
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

    # need some kind of manager registry that we can send this message to, I guess?
    GenServer.cast(:task_queue, {:new_manager, msg})
  end

  def handle_message(msg) do
    raise "Unsupported message"
  end
end

defmodule EIC.TaskQueue do
  use GenServer

  def start_link() do
    IO.puts("TaskQueue: start_link")
    GenServer.start_link(EIC.TaskQueue, [], name: :task_queue)
  end

  @impl true
  def init(_args) do
    IO.puts("TaskQueue: initializing")
    {:ok, %{:tasks => [], :managers => []}}
  end

  @impl true
  def handle_cast({:new_task, task_dict}, state) do
    IO.puts("TaskQueue: received a task")
    new_state = %{:tasks => [task_dict | state[:tasks]], :managers => state[:managers]}

    new_state2 = matchmake(new_state)
    {:noreply, new_state}
  end

  def handle_cast({:new_manager, registration_msg}, state) do
    new_state = %{:tasks => state[:tasks], :managers => [registration_msg | state[:managers]]}
    IO.puts("TaskQueue: new state:")
    IO.inspect(new_state)
    new_state2 = matchmake(new_state)
    {:noreply, new_state2}
  end

  def handle_cast(rest, state) do
    IO.puts("TaskQueue: ERROR: leftover handle_cast")
    IO.inspect(rest)
    raise "Unhandled TaskQueue cast"
  end

  @impl true
  def handle_call(_what, _from, state) do
    IO.puts("TaskQueue: ERROR: handle call")
    raise "Unhandled TaskQueue call"
  end

  # TODO: manager should be guarded by available capacity and new state should
  # modify that capacity, rather than forgetting the whole manager...
  # and maybe that means this can't be implemented in function guard style?
  def matchmake(%{:tasks => [t | t_rest], :managers => [m | m_rest]} = state) do
    IO.puts("Made a match")
    # TODO: send this off to execute
    # also, record the pairing somehow so that we do appropriate behaviour on
    # task result or manager failure.

    # getting that into the interchange->workers ZMQ socket seems pretty awkward
    # but maybe I can do it with an inproc: zmq message and a poller, so that
    # we use ZMQ messaging from inside matchmake instead of erlang messaging?

    %{:tasks => t_rest, :managers => m_rest}
  end

  def matchmake(state) do
    IO.puts("No match made between any manager or task")
    state
  end
end
