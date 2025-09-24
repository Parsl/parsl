import Config

config :logger,
  compile_time_purge_matching: [
     [level_lower_than: :debug]
  ],
  handle_sasl_reports: true

config :logger, :default_formatter,
  format: "$time [$level] $message $metadata\n",
  metadata: [:mfa, :file, :line, :pid],
  colors: [enabled: false]

config :logger, :default_handler,
  config: [sync_mode_qlen: 0, drop_mode_qlen: 1000000, flush_qlen: 1000000, burst_limit_enable: false, file: ~c"elixirchange.log"]

