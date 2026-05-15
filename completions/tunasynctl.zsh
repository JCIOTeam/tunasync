#compdef tunasynctl

autoload -U is-at-least

_tunasynctl() {
    typeset -A opt_args
    typeset -a _arguments_options
    local ret=1

    if is-at-least 5.2; then
        _arguments_options=(-s -S -C)
    else
        _arguments_options=(-s -C)
    fi

    local context curcontext="$curcontext" state line
    _arguments "${_arguments_options[@]}" : \
'-c+[Explicit config file (overrides system and user config files)]:CONFIG:_files' \
'--config=[Explicit config file (overrides system and user config files)]:CONFIG:_files' \
'-m+[Manager host or IP address]:MANAGER:_default' \
'--manager=[Manager host or IP address]:MANAGER:_default' \
'-p+[Manager port]:PORT:_default' \
'--port=[Manager port]:PORT:_default' \
'--ca-cert=[CA cert for TLS verification (enables HTTPS)]:CA_CERT:_files' \
'-v[Verbose logging]' \
'--verbose[Verbose logging]' \
'-h[Print help]' \
'--help[Print help]' \
'-V[Print version]' \
'--version[Print version]' \
":: :_tunasynctl_commands" \
"*::: :->tunasynctl" \
&& ret=0
    case $state in
    (tunasynctl)
        words=($line[1] "${words[@]}")
        (( CURRENT += 1 ))
        curcontext="${curcontext%:*:*}:tunasynctl-command-$line[1]:"
        case $line[1] in
            (list)
_arguments "${_arguments_options[@]}" : \
'-w+[List jobs of a specific worker; omit for all workers]:WORKER:_default' \
'--worker=[List jobs of a specific worker; omit for all workers]:WORKER:_default' \
'--status=[Filter by status (comma-separated\: syncing,failed,success,…)]:STATUS:_default' \
'--format=[Output format\: \`json\` (default) or \`table\`]:FORMAT:_default' \
'-c+[Explicit config file (overrides system and user config files)]:CONFIG:_files' \
'--config=[Explicit config file (overrides system and user config files)]:CONFIG:_files' \
'-m+[Manager host or IP address]:MANAGER:_default' \
'--manager=[Manager host or IP address]:MANAGER:_default' \
'-p+[Manager port]:PORT:_default' \
'--port=[Manager port]:PORT:_default' \
'--ca-cert=[CA cert for TLS verification (enables HTTPS)]:CA_CERT:_files' \
'--all[Show all workers'\'' jobs]' \
'-v[Verbose logging]' \
'--verbose[Verbose logging]' \
'-h[Print help]' \
'--help[Print help]' \
&& ret=0
;;
(workers)
_arguments "${_arguments_options[@]}" : \
'-c+[Explicit config file (overrides system and user config files)]:CONFIG:_files' \
'--config=[Explicit config file (overrides system and user config files)]:CONFIG:_files' \
'-m+[Manager host or IP address]:MANAGER:_default' \
'--manager=[Manager host or IP address]:MANAGER:_default' \
'-p+[Manager port]:PORT:_default' \
'--port=[Manager port]:PORT:_default' \
'--ca-cert=[CA cert for TLS verification (enables HTTPS)]:CA_CERT:_files' \
'-v[Verbose logging]' \
'--verbose[Verbose logging]' \
'-h[Print help]' \
'--help[Print help]' \
&& ret=0
;;
(flush)
_arguments "${_arguments_options[@]}" : \
'-c+[Explicit config file (overrides system and user config files)]:CONFIG:_files' \
'--config=[Explicit config file (overrides system and user config files)]:CONFIG:_files' \
'-m+[Manager host or IP address]:MANAGER:_default' \
'--manager=[Manager host or IP address]:MANAGER:_default' \
'-p+[Manager port]:PORT:_default' \
'--port=[Manager port]:PORT:_default' \
'--ca-cert=[CA cert for TLS verification (enables HTTPS)]:CA_CERT:_files' \
'-v[Verbose logging]' \
'--verbose[Verbose logging]' \
'-h[Print help]' \
'--help[Print help]' \
&& ret=0
;;
(rm-worker)
_arguments "${_arguments_options[@]}" : \
'-c+[Explicit config file (overrides system and user config files)]:CONFIG:_files' \
'--config=[Explicit config file (overrides system and user config files)]:CONFIG:_files' \
'-m+[Manager host or IP address]:MANAGER:_default' \
'--manager=[Manager host or IP address]:MANAGER:_default' \
'-p+[Manager port]:PORT:_default' \
'--port=[Manager port]:PORT:_default' \
'--ca-cert=[CA cert for TLS verification (enables HTTPS)]:CA_CERT:_files' \
'-v[Verbose logging]' \
'--verbose[Verbose logging]' \
'-h[Print help]' \
'--help[Print help]' \
':worker -- Worker ID:_default' \
&& ret=0
;;
(set-size)
_arguments "${_arguments_options[@]}" : \
'-w+[Restrict to a specific worker]:WORKER:_default' \
'--worker=[Restrict to a specific worker]:WORKER:_default' \
'-c+[Explicit config file (overrides system and user config files)]:CONFIG:_files' \
'--config=[Explicit config file (overrides system and user config files)]:CONFIG:_files' \
'-m+[Manager host or IP address]:MANAGER:_default' \
'--manager=[Manager host or IP address]:MANAGER:_default' \
'-p+[Manager port]:PORT:_default' \
'--port=[Manager port]:PORT:_default' \
'--ca-cert=[CA cert for TLS verification (enables HTTPS)]:CA_CERT:_files' \
'-v[Verbose logging]' \
'--verbose[Verbose logging]' \
'-h[Print help]' \
'--help[Print help]' \
':mirror -- Mirror name:_default' \
':size -- Human-readable size string, e.g. `1.2T`:_default' \
&& ret=0
;;
(start)
_arguments "${_arguments_options[@]}" : \
'-w+[Restrict to a specific worker]:WORKER:_default' \
'--worker=[Restrict to a specific worker]:WORKER:_default' \
'-c+[Explicit config file (overrides system and user config files)]:CONFIG:_files' \
'--config=[Explicit config file (overrides system and user config files)]:CONFIG:_files' \
'-m+[Manager host or IP address]:MANAGER:_default' \
'--manager=[Manager host or IP address]:MANAGER:_default' \
'-p+[Manager port]:PORT:_default' \
'--port=[Manager port]:PORT:_default' \
'--ca-cert=[CA cert for TLS verification (enables HTTPS)]:CA_CERT:_files' \
'-f[Ignore concurrency limit (force-start)]' \
'--force[Ignore concurrency limit (force-start)]' \
'-v[Verbose logging]' \
'--verbose[Verbose logging]' \
'-h[Print help]' \
'--help[Print help]' \
':mirror -- Mirror name, or `all` to broadcast:_default' \
&& ret=0
;;
(stop)
_arguments "${_arguments_options[@]}" : \
'-w+[]:WORKER:_default' \
'--worker=[]:WORKER:_default' \
'-c+[Explicit config file (overrides system and user config files)]:CONFIG:_files' \
'--config=[Explicit config file (overrides system and user config files)]:CONFIG:_files' \
'-m+[Manager host or IP address]:MANAGER:_default' \
'--manager=[Manager host or IP address]:MANAGER:_default' \
'-p+[Manager port]:PORT:_default' \
'--port=[Manager port]:PORT:_default' \
'--ca-cert=[CA cert for TLS verification (enables HTTPS)]:CA_CERT:_files' \
'-v[Verbose logging]' \
'--verbose[Verbose logging]' \
'-h[Print help]' \
'--help[Print help]' \
':mirror:_default' \
&& ret=0
;;
(disable)
_arguments "${_arguments_options[@]}" : \
'-w+[]:WORKER:_default' \
'--worker=[]:WORKER:_default' \
'-c+[Explicit config file (overrides system and user config files)]:CONFIG:_files' \
'--config=[Explicit config file (overrides system and user config files)]:CONFIG:_files' \
'-m+[Manager host or IP address]:MANAGER:_default' \
'--manager=[Manager host or IP address]:MANAGER:_default' \
'-p+[Manager port]:PORT:_default' \
'--port=[Manager port]:PORT:_default' \
'--ca-cert=[CA cert for TLS verification (enables HTTPS)]:CA_CERT:_files' \
'-v[Verbose logging]' \
'--verbose[Verbose logging]' \
'-h[Print help]' \
'--help[Print help]' \
':mirror:_default' \
&& ret=0
;;
(restart)
_arguments "${_arguments_options[@]}" : \
'-w+[]:WORKER:_default' \
'--worker=[]:WORKER:_default' \
'-c+[Explicit config file (overrides system and user config files)]:CONFIG:_files' \
'--config=[Explicit config file (overrides system and user config files)]:CONFIG:_files' \
'-m+[Manager host or IP address]:MANAGER:_default' \
'--manager=[Manager host or IP address]:MANAGER:_default' \
'-p+[Manager port]:PORT:_default' \
'--port=[Manager port]:PORT:_default' \
'--ca-cert=[CA cert for TLS verification (enables HTTPS)]:CA_CERT:_files' \
'-v[Verbose logging]' \
'--verbose[Verbose logging]' \
'-h[Print help]' \
'--help[Print help]' \
':mirror:_default' \
&& ret=0
;;
(reload)
_arguments "${_arguments_options[@]}" : \
'-c+[Explicit config file (overrides system and user config files)]:CONFIG:_files' \
'--config=[Explicit config file (overrides system and user config files)]:CONFIG:_files' \
'-m+[Manager host or IP address]:MANAGER:_default' \
'--manager=[Manager host or IP address]:MANAGER:_default' \
'-p+[Manager port]:PORT:_default' \
'--port=[Manager port]:PORT:_default' \
'--ca-cert=[CA cert for TLS verification (enables HTTPS)]:CA_CERT:_files' \
'-v[Verbose logging]' \
'--verbose[Verbose logging]' \
'-h[Print help]' \
'--help[Print help]' \
':worker -- Worker ID:_default' \
&& ret=0
;;
(completion)
_arguments "${_arguments_options[@]}" : \
'-c+[Explicit config file (overrides system and user config files)]:CONFIG:_files' \
'--config=[Explicit config file (overrides system and user config files)]:CONFIG:_files' \
'-m+[Manager host or IP address]:MANAGER:_default' \
'--manager=[Manager host or IP address]:MANAGER:_default' \
'-p+[Manager port]:PORT:_default' \
'--port=[Manager port]:PORT:_default' \
'--ca-cert=[CA cert for TLS verification (enables HTTPS)]:CA_CERT:_files' \
'-v[Verbose logging]' \
'--verbose[Verbose logging]' \
'-h[Print help (see more with '\''--help'\'')]' \
'--help[Print help (see more with '\''--help'\'')]' \
':shell -- Shell type:(bash elvish fish powershell zsh)' \
&& ret=0
;;
(help)
_arguments "${_arguments_options[@]}" : \
":: :_tunasynctl__subcmd__help_commands" \
"*::: :->help" \
&& ret=0

    case $state in
    (help)
        words=($line[1] "${words[@]}")
        (( CURRENT += 1 ))
        curcontext="${curcontext%:*:*}:tunasynctl-help-command-$line[1]:"
        case $line[1] in
            (list)
_arguments "${_arguments_options[@]}" : \
&& ret=0
;;
(workers)
_arguments "${_arguments_options[@]}" : \
&& ret=0
;;
(flush)
_arguments "${_arguments_options[@]}" : \
&& ret=0
;;
(rm-worker)
_arguments "${_arguments_options[@]}" : \
&& ret=0
;;
(set-size)
_arguments "${_arguments_options[@]}" : \
&& ret=0
;;
(start)
_arguments "${_arguments_options[@]}" : \
&& ret=0
;;
(stop)
_arguments "${_arguments_options[@]}" : \
&& ret=0
;;
(disable)
_arguments "${_arguments_options[@]}" : \
&& ret=0
;;
(restart)
_arguments "${_arguments_options[@]}" : \
&& ret=0
;;
(reload)
_arguments "${_arguments_options[@]}" : \
&& ret=0
;;
(completion)
_arguments "${_arguments_options[@]}" : \
&& ret=0
;;
(help)
_arguments "${_arguments_options[@]}" : \
&& ret=0
;;
        esac
    ;;
esac
;;
        esac
    ;;
esac
}

(( $+functions[_tunasynctl_commands] )) ||
_tunasynctl_commands() {
    local commands; commands=(
'list:List all mirror jobs' \
'workers:List all registered workers' \
'flush:Flush all disabled job rows from the manager DB' \
'rm-worker:Remove a worker from the manager' \
'set-size:Update the size of a mirror (operator override)' \
'start:Start a mirror job' \
'stop:Stop a running mirror job' \
'disable:Disable a mirror job' \
'restart:Restart a mirror job' \
'reload:Tell a worker to reload its config from disk' \
'completion:Generate shell completion script' \
'help:Print this message or the help of the given subcommand(s)' \
    )
    _describe -t commands 'tunasynctl commands' commands "$@"
}
(( $+functions[_tunasynctl__subcmd__completion_commands] )) ||
_tunasynctl__subcmd__completion_commands() {
    local commands; commands=()
    _describe -t commands 'tunasynctl completion commands' commands "$@"
}
(( $+functions[_tunasynctl__subcmd__disable_commands] )) ||
_tunasynctl__subcmd__disable_commands() {
    local commands; commands=()
    _describe -t commands 'tunasynctl disable commands' commands "$@"
}
(( $+functions[_tunasynctl__subcmd__flush_commands] )) ||
_tunasynctl__subcmd__flush_commands() {
    local commands; commands=()
    _describe -t commands 'tunasynctl flush commands' commands "$@"
}
(( $+functions[_tunasynctl__subcmd__help_commands] )) ||
_tunasynctl__subcmd__help_commands() {
    local commands; commands=(
'list:List all mirror jobs' \
'workers:List all registered workers' \
'flush:Flush all disabled job rows from the manager DB' \
'rm-worker:Remove a worker from the manager' \
'set-size:Update the size of a mirror (operator override)' \
'start:Start a mirror job' \
'stop:Stop a running mirror job' \
'disable:Disable a mirror job' \
'restart:Restart a mirror job' \
'reload:Tell a worker to reload its config from disk' \
'completion:Generate shell completion script' \
'help:Print this message or the help of the given subcommand(s)' \
    )
    _describe -t commands 'tunasynctl help commands' commands "$@"
}
(( $+functions[_tunasynctl__subcmd__help__subcmd__completion_commands] )) ||
_tunasynctl__subcmd__help__subcmd__completion_commands() {
    local commands; commands=()
    _describe -t commands 'tunasynctl help completion commands' commands "$@"
}
(( $+functions[_tunasynctl__subcmd__help__subcmd__disable_commands] )) ||
_tunasynctl__subcmd__help__subcmd__disable_commands() {
    local commands; commands=()
    _describe -t commands 'tunasynctl help disable commands' commands "$@"
}
(( $+functions[_tunasynctl__subcmd__help__subcmd__flush_commands] )) ||
_tunasynctl__subcmd__help__subcmd__flush_commands() {
    local commands; commands=()
    _describe -t commands 'tunasynctl help flush commands' commands "$@"
}
(( $+functions[_tunasynctl__subcmd__help__subcmd__help_commands] )) ||
_tunasynctl__subcmd__help__subcmd__help_commands() {
    local commands; commands=()
    _describe -t commands 'tunasynctl help help commands' commands "$@"
}
(( $+functions[_tunasynctl__subcmd__help__subcmd__list_commands] )) ||
_tunasynctl__subcmd__help__subcmd__list_commands() {
    local commands; commands=()
    _describe -t commands 'tunasynctl help list commands' commands "$@"
}
(( $+functions[_tunasynctl__subcmd__help__subcmd__reload_commands] )) ||
_tunasynctl__subcmd__help__subcmd__reload_commands() {
    local commands; commands=()
    _describe -t commands 'tunasynctl help reload commands' commands "$@"
}
(( $+functions[_tunasynctl__subcmd__help__subcmd__restart_commands] )) ||
_tunasynctl__subcmd__help__subcmd__restart_commands() {
    local commands; commands=()
    _describe -t commands 'tunasynctl help restart commands' commands "$@"
}
(( $+functions[_tunasynctl__subcmd__help__subcmd__rm-worker_commands] )) ||
_tunasynctl__subcmd__help__subcmd__rm-worker_commands() {
    local commands; commands=()
    _describe -t commands 'tunasynctl help rm-worker commands' commands "$@"
}
(( $+functions[_tunasynctl__subcmd__help__subcmd__set-size_commands] )) ||
_tunasynctl__subcmd__help__subcmd__set-size_commands() {
    local commands; commands=()
    _describe -t commands 'tunasynctl help set-size commands' commands "$@"
}
(( $+functions[_tunasynctl__subcmd__help__subcmd__start_commands] )) ||
_tunasynctl__subcmd__help__subcmd__start_commands() {
    local commands; commands=()
    _describe -t commands 'tunasynctl help start commands' commands "$@"
}
(( $+functions[_tunasynctl__subcmd__help__subcmd__stop_commands] )) ||
_tunasynctl__subcmd__help__subcmd__stop_commands() {
    local commands; commands=()
    _describe -t commands 'tunasynctl help stop commands' commands "$@"
}
(( $+functions[_tunasynctl__subcmd__help__subcmd__workers_commands] )) ||
_tunasynctl__subcmd__help__subcmd__workers_commands() {
    local commands; commands=()
    _describe -t commands 'tunasynctl help workers commands' commands "$@"
}
(( $+functions[_tunasynctl__subcmd__list_commands] )) ||
_tunasynctl__subcmd__list_commands() {
    local commands; commands=()
    _describe -t commands 'tunasynctl list commands' commands "$@"
}
(( $+functions[_tunasynctl__subcmd__reload_commands] )) ||
_tunasynctl__subcmd__reload_commands() {
    local commands; commands=()
    _describe -t commands 'tunasynctl reload commands' commands "$@"
}
(( $+functions[_tunasynctl__subcmd__restart_commands] )) ||
_tunasynctl__subcmd__restart_commands() {
    local commands; commands=()
    _describe -t commands 'tunasynctl restart commands' commands "$@"
}
(( $+functions[_tunasynctl__subcmd__rm-worker_commands] )) ||
_tunasynctl__subcmd__rm-worker_commands() {
    local commands; commands=()
    _describe -t commands 'tunasynctl rm-worker commands' commands "$@"
}
(( $+functions[_tunasynctl__subcmd__set-size_commands] )) ||
_tunasynctl__subcmd__set-size_commands() {
    local commands; commands=()
    _describe -t commands 'tunasynctl set-size commands' commands "$@"
}
(( $+functions[_tunasynctl__subcmd__start_commands] )) ||
_tunasynctl__subcmd__start_commands() {
    local commands; commands=()
    _describe -t commands 'tunasynctl start commands' commands "$@"
}
(( $+functions[_tunasynctl__subcmd__stop_commands] )) ||
_tunasynctl__subcmd__stop_commands() {
    local commands; commands=()
    _describe -t commands 'tunasynctl stop commands' commands "$@"
}
(( $+functions[_tunasynctl__subcmd__workers_commands] )) ||
_tunasynctl__subcmd__workers_commands() {
    local commands; commands=()
    _describe -t commands 'tunasynctl workers commands' commands "$@"
}

if [ "$funcstack[1]" = "_tunasynctl" ]; then
    _tunasynctl "$@"
else
    compdef _tunasynctl tunasynctl
fi
