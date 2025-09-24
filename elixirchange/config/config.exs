import Config

config :logger,
  compile_time_purge_matching: [
     [level_lower_than: :debug]
  ]

config :logger, :default_formatter,
  format: "$time [$level] $message $metadata\n",
  metadata: [:mfa, :file, :line, :pid]
