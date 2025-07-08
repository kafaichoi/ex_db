import Config

# Runtime configuration
if config_env() == :prod do
  config :ex_db,
    port: String.to_integer(System.get_env("EX_DB_PORT") || "5432")
end
