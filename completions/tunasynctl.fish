# Print an optspec for argparse to handle cmd's options that are independent of any subcommand.
function __fish_tunasynctl_global_optspecs
	string join \n c/config= m/manager= p/port= ca-cert= v/verbose h/help V/version
end

function __fish_tunasynctl_needs_command
	# Figure out if the current invocation already has a command.
	set -l cmd (commandline -opc)
	set -e cmd[1]
	argparse -s (__fish_tunasynctl_global_optspecs) -- $cmd 2>/dev/null
	or return
	if set -q argv[1]
		# Also print the command, so this can be used to figure out what it is.
		echo $argv[1]
		return 1
	end
	return 0
end

function __fish_tunasynctl_using_subcommand
	set -l cmd (__fish_tunasynctl_needs_command)
	test -z "$cmd"
	and return 1
	contains -- $cmd[1] $argv
end

complete -c tunasynctl -n "__fish_tunasynctl_needs_command" -s c -l config -d 'Explicit config file (overrides system and user config files)' -r -F
complete -c tunasynctl -n "__fish_tunasynctl_needs_command" -s m -l manager -d 'Manager host or IP address' -r
complete -c tunasynctl -n "__fish_tunasynctl_needs_command" -s p -l port -d 'Manager port' -r
complete -c tunasynctl -n "__fish_tunasynctl_needs_command" -l ca-cert -d 'CA cert for TLS verification (enables HTTPS)' -r -F
complete -c tunasynctl -n "__fish_tunasynctl_needs_command" -s v -l verbose -d 'Verbose logging'
complete -c tunasynctl -n "__fish_tunasynctl_needs_command" -s h -l help -d 'Print help'
complete -c tunasynctl -n "__fish_tunasynctl_needs_command" -s V -l version -d 'Print version'
complete -c tunasynctl -n "__fish_tunasynctl_needs_command" -f -a "list" -d 'List all mirror jobs'
complete -c tunasynctl -n "__fish_tunasynctl_needs_command" -f -a "workers" -d 'List all registered workers'
complete -c tunasynctl -n "__fish_tunasynctl_needs_command" -f -a "flush" -d 'Flush all disabled job rows from the manager DB'
complete -c tunasynctl -n "__fish_tunasynctl_needs_command" -f -a "rm-worker" -d 'Remove a worker from the manager'
complete -c tunasynctl -n "__fish_tunasynctl_needs_command" -f -a "set-size" -d 'Update the size of a mirror (operator override)'
complete -c tunasynctl -n "__fish_tunasynctl_needs_command" -f -a "start" -d 'Start a mirror job'
complete -c tunasynctl -n "__fish_tunasynctl_needs_command" -f -a "stop" -d 'Stop a running mirror job'
complete -c tunasynctl -n "__fish_tunasynctl_needs_command" -f -a "disable" -d 'Disable a mirror job'
complete -c tunasynctl -n "__fish_tunasynctl_needs_command" -f -a "restart" -d 'Restart a mirror job'
complete -c tunasynctl -n "__fish_tunasynctl_needs_command" -f -a "reload" -d 'Tell a worker to reload its config from disk'
complete -c tunasynctl -n "__fish_tunasynctl_needs_command" -f -a "completion" -d 'Generate shell completion script'
complete -c tunasynctl -n "__fish_tunasynctl_needs_command" -f -a "help" -d 'Print this message or the help of the given subcommand(s)'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand list" -s w -l worker -d 'List jobs of a specific worker; omit for all workers' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand list" -l status -d 'Filter by status (comma-separated: syncing,failed,success,…)' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand list" -l format -d 'Output format: `json` (default) or `table`' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand list" -s c -l config -d 'Explicit config file (overrides system and user config files)' -r -F
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand list" -s m -l manager -d 'Manager host or IP address' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand list" -s p -l port -d 'Manager port' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand list" -l ca-cert -d 'CA cert for TLS verification (enables HTTPS)' -r -F
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand list" -l all -d 'Show all workers\' jobs'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand list" -s v -l verbose -d 'Verbose logging'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand list" -s h -l help -d 'Print help'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand workers" -s c -l config -d 'Explicit config file (overrides system and user config files)' -r -F
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand workers" -s m -l manager -d 'Manager host or IP address' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand workers" -s p -l port -d 'Manager port' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand workers" -l ca-cert -d 'CA cert for TLS verification (enables HTTPS)' -r -F
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand workers" -s v -l verbose -d 'Verbose logging'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand workers" -s h -l help -d 'Print help'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand flush" -s c -l config -d 'Explicit config file (overrides system and user config files)' -r -F
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand flush" -s m -l manager -d 'Manager host or IP address' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand flush" -s p -l port -d 'Manager port' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand flush" -l ca-cert -d 'CA cert for TLS verification (enables HTTPS)' -r -F
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand flush" -s v -l verbose -d 'Verbose logging'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand flush" -s h -l help -d 'Print help'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand rm-worker" -s c -l config -d 'Explicit config file (overrides system and user config files)' -r -F
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand rm-worker" -s m -l manager -d 'Manager host or IP address' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand rm-worker" -s p -l port -d 'Manager port' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand rm-worker" -l ca-cert -d 'CA cert for TLS verification (enables HTTPS)' -r -F
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand rm-worker" -s v -l verbose -d 'Verbose logging'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand rm-worker" -s h -l help -d 'Print help'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand set-size" -s w -l worker -d 'Restrict to a specific worker' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand set-size" -s c -l config -d 'Explicit config file (overrides system and user config files)' -r -F
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand set-size" -s m -l manager -d 'Manager host or IP address' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand set-size" -s p -l port -d 'Manager port' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand set-size" -l ca-cert -d 'CA cert for TLS verification (enables HTTPS)' -r -F
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand set-size" -s v -l verbose -d 'Verbose logging'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand set-size" -s h -l help -d 'Print help'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand start" -s w -l worker -d 'Restrict to a specific worker' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand start" -s c -l config -d 'Explicit config file (overrides system and user config files)' -r -F
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand start" -s m -l manager -d 'Manager host or IP address' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand start" -s p -l port -d 'Manager port' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand start" -l ca-cert -d 'CA cert for TLS verification (enables HTTPS)' -r -F
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand start" -s f -l force -d 'Ignore concurrency limit (force-start)'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand start" -s v -l verbose -d 'Verbose logging'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand start" -s h -l help -d 'Print help'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand stop" -s w -l worker -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand stop" -s c -l config -d 'Explicit config file (overrides system and user config files)' -r -F
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand stop" -s m -l manager -d 'Manager host or IP address' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand stop" -s p -l port -d 'Manager port' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand stop" -l ca-cert -d 'CA cert for TLS verification (enables HTTPS)' -r -F
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand stop" -s v -l verbose -d 'Verbose logging'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand stop" -s h -l help -d 'Print help'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand disable" -s w -l worker -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand disable" -s c -l config -d 'Explicit config file (overrides system and user config files)' -r -F
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand disable" -s m -l manager -d 'Manager host or IP address' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand disable" -s p -l port -d 'Manager port' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand disable" -l ca-cert -d 'CA cert for TLS verification (enables HTTPS)' -r -F
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand disable" -s v -l verbose -d 'Verbose logging'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand disable" -s h -l help -d 'Print help'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand restart" -s w -l worker -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand restart" -s c -l config -d 'Explicit config file (overrides system and user config files)' -r -F
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand restart" -s m -l manager -d 'Manager host or IP address' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand restart" -s p -l port -d 'Manager port' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand restart" -l ca-cert -d 'CA cert for TLS verification (enables HTTPS)' -r -F
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand restart" -s v -l verbose -d 'Verbose logging'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand restart" -s h -l help -d 'Print help'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand reload" -s c -l config -d 'Explicit config file (overrides system and user config files)' -r -F
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand reload" -s m -l manager -d 'Manager host or IP address' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand reload" -s p -l port -d 'Manager port' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand reload" -l ca-cert -d 'CA cert for TLS verification (enables HTTPS)' -r -F
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand reload" -s v -l verbose -d 'Verbose logging'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand reload" -s h -l help -d 'Print help'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand completion" -s c -l config -d 'Explicit config file (overrides system and user config files)' -r -F
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand completion" -s m -l manager -d 'Manager host or IP address' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand completion" -s p -l port -d 'Manager port' -r
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand completion" -l ca-cert -d 'CA cert for TLS verification (enables HTTPS)' -r -F
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand completion" -s v -l verbose -d 'Verbose logging'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand completion" -s h -l help -d 'Print help (see more with \'--help\')'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand help; and not __fish_seen_subcommand_from list workers flush rm-worker set-size start stop disable restart reload completion help" -f -a "list" -d 'List all mirror jobs'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand help; and not __fish_seen_subcommand_from list workers flush rm-worker set-size start stop disable restart reload completion help" -f -a "workers" -d 'List all registered workers'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand help; and not __fish_seen_subcommand_from list workers flush rm-worker set-size start stop disable restart reload completion help" -f -a "flush" -d 'Flush all disabled job rows from the manager DB'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand help; and not __fish_seen_subcommand_from list workers flush rm-worker set-size start stop disable restart reload completion help" -f -a "rm-worker" -d 'Remove a worker from the manager'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand help; and not __fish_seen_subcommand_from list workers flush rm-worker set-size start stop disable restart reload completion help" -f -a "set-size" -d 'Update the size of a mirror (operator override)'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand help; and not __fish_seen_subcommand_from list workers flush rm-worker set-size start stop disable restart reload completion help" -f -a "start" -d 'Start a mirror job'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand help; and not __fish_seen_subcommand_from list workers flush rm-worker set-size start stop disable restart reload completion help" -f -a "stop" -d 'Stop a running mirror job'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand help; and not __fish_seen_subcommand_from list workers flush rm-worker set-size start stop disable restart reload completion help" -f -a "disable" -d 'Disable a mirror job'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand help; and not __fish_seen_subcommand_from list workers flush rm-worker set-size start stop disable restart reload completion help" -f -a "restart" -d 'Restart a mirror job'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand help; and not __fish_seen_subcommand_from list workers flush rm-worker set-size start stop disable restart reload completion help" -f -a "reload" -d 'Tell a worker to reload its config from disk'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand help; and not __fish_seen_subcommand_from list workers flush rm-worker set-size start stop disable restart reload completion help" -f -a "completion" -d 'Generate shell completion script'
complete -c tunasynctl -n "__fish_tunasynctl_using_subcommand help; and not __fish_seen_subcommand_from list workers flush rm-worker set-size start stop disable restart reload completion help" -f -a "help" -d 'Print this message or the help of the given subcommand(s)'
