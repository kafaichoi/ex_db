import Config

# Configure Logger to show metadata for better debugging
config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: :all

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{config_env()}.exs"
