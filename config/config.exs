import Config

# Configure your application
config :ex_db,
  port: String.to_integer(System.get_env("EX_DB_PORT") || "5432")

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{config_env()}.exs"
