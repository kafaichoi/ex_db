import Config

# Runtime configuration
config :ex_db,
  port: String.to_integer(System.get_env("EX_DB_PORT") || "5432")
