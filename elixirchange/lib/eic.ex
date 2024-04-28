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

    children = [ %{id: EIC.TasksSubmitToInterchange, start: {EIC.TasksSubmitToInterchange, :start_link, []}},
                 # %{id: EIC.TaskQueue, start: {GenServer, :start_link, [EIC.TaskQueue, []]}}
                 %{id: EIC.TaskQueue, start: {EIC.TaskQueue, :start_link, []}}
               ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule EIC.TasksSubmitToInterchange do
  @moduledoc """
  This module handles tasks coming from the submit side into the interchange
  over ZMQ.
  """

  def start_link() do
      Task.start_link(EIC.TasksSubmitToInterchange, :body, [])
  end

  def body() do
      IO.puts "Starting tasks submit to interchange ZMQ handler"
      {:ok, ctx} = :erlzmq.context()
      {:ok, socket} = :erlzmq.socket(ctx, :dealer)
      :ok = :erlzmq.connect(socket, "tcp://127.0.0.1:9000")
      loop(socket)
  end

  def loop(socket) do
      IO.puts "Invoking task receive"
      {:ok, msg} = :erlzmq.recv(socket)
      # msg is a pickled dict with keys: task_id and buffer
      IO.inspect(msg)
      {task_dict, ""} = Unpickler.load!(msg)
      IO.inspect(task_dict)

      IO.puts "casting task to task queue"
      GenServer.cast(:tq, {:push, task_dict})

      loop(socket)
  end
end

defmodule EIC.TaskQueue do
  use GenServer

  def start_link() do
    IO.puts "TaskQueue: start_link"
    GenServer.start_link(EIC.TaskQueue, [], name: :tq)
  end

  @impl true
  def init(_args) do
    IO.puts "TaskQueue: initializing"
    {:ok, []}
  end 

  @impl true
  def handle_cast({:push, task_dict}, state) do
    IO.puts "TaskQueue: received a task"
    {:noreply, [task_dict | state]}
  end

  def handle_cast(rest, state) do
    IO.puts "TaskQueue: ERROR: leftover handle_cast"
    IO.inspect(rest)
    {:noreply, state}
  end

  @impl true
  def handle_call(_what, _from, state) do
    IO.puts "TaskQueue: ERROR: handle call"
    {:reply, state, state}
  end

end
