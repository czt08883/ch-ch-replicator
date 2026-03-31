This should be linux cli tool for replicating one standalone clickhouse database into another standalone clickhouse database.
 That is it should take source and target clickhouses as a mandatory cli parameters, something like this:

   ./ch-ch-replicator --src=SOURCE_CLICKHOUSE_DSN --dest=DESTINATION_CLICKHOUSE_DSN --threads=THREADS

 where clickhouse DSN is in format:

   clickhouse://<user>:<password>@<host>:<port>/<database>?<options>

 where "?<options>" is optional, everything else is mandatory.

 It should perform target database and tables creation if those does not exist.
 It should work with all tables in source DB, but it should skip views or materialized views.
 It should perform initial sync of existing data if target does not contain that data using multiple worker threads.
 Number of those threads could be specified as a parameter. Assume single worker thread if parameter is omitted.
 After initial sync it should switch to CDC mode and stay in this mode indefinitely until manually stopped by SIGINT or SIGTERM.
 Those signals should initiate graceful shutdown, that is any pending operations should be complete before exitting.
 All steps of this tool should be idempotent, that is it could be stopped anytime. Next time it is launched it should re-check if any of
 steps is incomplete or missing and perform (or continue) those steps properly.

 This tool should be written in Rust using Tokio for async operations.

