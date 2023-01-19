%% @author Eric des Courtis <eric.descourtis@fireworkhq.com>
%% @doc eharbor is an Erlang library that allows for running a
%% function in a controlled manner, with the ability to deduplicate
%% function calls with the same arguments. It features configurable
%% deduplication, ordered function calls, and buffering to prevent
%% system overload. This library is useful for running expensive tasks,
%% where deduplication of the same inputs can lead to significant
%% performance gains.
%%
%% The config type is a map with the following keys:
%%
%% `name' (default: `default') The name of the harbor instance. This name is used to derive
%%        a coordinator name and a pobox name.
%%
%% `backlog' (default: `1000') How big should our pobox buffer be? This is
%%           effectively going to determine how much memory the pobox will
%%           be allowed to consume. The underlying pobox acts as our load
%%           shedding for overload scenarios. Don't set this parameter to
%%           an arbitrary value instead try to calculate how much memory will
%%           use when the pobox will be full to make sure that the system
%%           will survive.
%%
%% `piers' (default: `100') How many unique parameters to process at once. This
%%         effectively limits concurrency on the backend. It can be used as a
%%         bulkhead by providing a limit that is smaller than connection pool
%%         size for a database like Postgres for example.
%%
%% `breakwater_limit' (default: `2000') Maximum number of processes waiting
%%                    on a result being processed by the backend. This does
%%                    not include the `backlog' processes. Keep in mind this
%%                    value should not be to large otherwise memory spikes
%%                    could occur during fanout of the results from conductor
%%                    to follower.
%%
%% `dedup' (default: `true') Is this instance being used as a simple bulkhead
%%         or are we tring to accelerate the requests by deduplicating them.
%%
%% `ordered' (default: `true') For a sequential process P where no other sequential
%%           process writes. If the process reads and then writes when this parameter
%%           is set to false the write may appear to have occured before the read.
%%           This results in lower latency and lower overhead at the cost of ordering.
%%           The parameter is set to true by default since it that behavior is
%%           more intuitive. But if maximum performance is desired set it to false.
%%
%% `group_by_key_fun' (default: `fun(Key) -> Key end') Sometimes some parameters need
%%                    to be passed into harbor but shouldn't be considered for
%%                    pier assignment or deduplication. This allows you to ignore
%%                    some parameters while still passing them into your function.
%%
%% `error_type' (default: `value') How to handle scenarios where the pobox is full
%%              what type of error to return (should be one of these `throw | error | raise | exit | value').
%%
%% `error_value' (default: `{error, full}') The value of the throw error, exit etc.
%%               In this case we return a value because the default is `value'.
%%
%% `buffer_insert_timeout' (default: `5000') How long to wait for insertion into the
%%                         pobox buffer normally changing this should not be required.
%%
%% `follower_wait_for_conductor_timeout' (default: `infinity') How long the follower should
%%                                       wait after the conductor. Normally you shouldn't
%%                                       have to modify this value since crashes are detected
%%                                       and will not cause followers to wait until timeout.
%%
%% `conductor_wait_for_coordinator_followers_timeout' (default: `infinity') Don't touch unless
%%                                                    you know exactly what you are doing.
%% `assign_role_timeout' (default: `infinity') Don't touch unless you know exactly what you
%%                       are doing.
%%
-module(eharbor).

-export([run/2, run/3, config_defaults/0, merge_with_defaults/1, pobox_name/1,
         coordinator_name/1]).

-export_type([incomplete_config/0, config/0]).

-type config() ::
    #{name := atom(),
      backlog := non_neg_integer(),
      piers := non_neg_integer(),
      breakwater_limit := non_neg_integer(),
      dedup := boolean(),
      ordered := boolean(),
      group_by_key_fun := fun((any()) -> any()),
      error_type := throw | error | raise | exit | value,
      error_value := any(),
      buffer_insert_timeout := timeout(),
      follower_wait_for_conductor_timeout := timeout(),
      conductor_wait_for_coordinator_followers_timeout := timeout(),
      assign_role_timeout := timeout()}.
-type incomplete_config() ::
    #{name => atom(),
      backlog => non_neg_integer(),
      piers => non_neg_integer(),
      breakwater_limit => non_neg_integer(),
      dedup => boolean(),
      ordered => boolean(),
      group_by_key_fun => fun((any()) -> any()),
      error_type => throw | error | raise | exit | value,
      error_value => any(),
      buffer_insert_timeout => timeout(),
      follower_wait_for_conductor_timeout => timeout(),
      conductor_wait_for_coordinator_followers_timeout => timeout(),
      assign_role_timeout => timeout()}.

-define(CONFIG_DEFAULTS,
        #{name => default,
          backlog => 1000,
          piers => 100,
          breakwater_limit => 2000,
          dedup => true,
          ordered => true,
          group_by_key_fun => fun(Key) -> Key end,
          error_type => value,
          error_value => {error, full},
          buffer_insert_timeout => 5000,
          follower_wait_for_conductor_timeout => infinity,
          conductor_wait_for_coordinator_followers_timeout => infinity,
          assign_role_timeout => infinity}).

%% @doc start_link the harbor instance using a configuration
%%
%% @end
-spec start_link(Config :: config()) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(Config) ->
    eharbor_coordinator:start_link(Config).

%% @doc Return the configuration defaults for harbor.
%%
%% `config_defaults/2' returns the default configuration parameters.
%% This is later used to convert an `incomplete_config()' into a
%% `config()'.
%%
%% @see merge_with_defaults/1
%% @end
-spec config_defaults() -> config().
config_defaults() ->
    ?CONFIG_DEFAULTS.

%% @doc Merge an incomplete configuration with the defaults to produce a proper config.
%%
%% `merge_with_defaults/2' returns overridden parameters merged with the default
%%  configuration parameters when the paramter is missing.
%%
%% @see config_defaults/0
%% @end
-spec merge_with_defaults(incomplete_config()) -> config().
merge_with_defaults(Map) when is_map(Map) ->
    maps:merge_with(fun(_Key, Value1, Value2) ->
                       case Value2 of
                           undefined ->
                               Value1;
                           _ ->
                               Value2
                       end
                    end,
                    eharbor:config_defaults(),
                    Map).

%% @doc Run a function using a function reference and arguments with the
%%      config using a harbor instance.
%%
%% @end
-spec run(fun((any()) -> any()), [any()], config()) -> any() | no_return().
run(Function, Arguments, Config = #{ordered := Ordered}) ->
    do_run(Function, Arguments, Config, Ordered).

do_run(Function,
       Arguments,
       Config =
           #{name := Name,
             buffer_insert_timeout := BufferInsertTimeout,
             follower_wait_for_conductor_timeout := FollowerWaitForConductorTimeout,
             conductor_wait_for_coordinator_followers_timeout :=
                 ConductorWaitForCoordinatorFollowersTimeout,
             assign_role_timeout := AssignRoleTimeout,
             group_by_key_fun := GroupByKeyFun,
             error_type := ErrorType,
             error_value := ErrorValue},
       Ordered) ->
    PoboxName = pobox_name(Name),
    CoordinatorName = coordinator_name(Name),
    %% Start monitoring the coordinator before we start so we don't
    %% wait forever if it dies
    MRef = erlang:monitor(process, erlang:whereis(CoordinatorName)),

    %% Ask pobox to store our arguments (transformed by group_by_key_fun)
    %% This is how we dock into the harbor. If the buffer is full it will
    %% return :full
    case pobox:post_sync(PoboxName,
                         {'$habor_dock', {self(), MRef}, apply(GroupByKeyFun, Arguments)},
                         BufferInsertTimeout)
    of
        %% We successfully docked into the harbor
        ok ->
            %% Wait until we are assigned a role
            receive
                {MRef, {follower, ConductorPid}} ->
                    %% We got assigned a follower role lets monitor the conductor for this pier
                    %% since he is doing the work
                    ConductorMRef = erlang:monitor(process, ConductorPid),
                    receive
                        {MRef, Result0} ->
                            %% We have our result computed by the conductor
                            %% We can stop monitoring and flush monitoring messages
                            erlang:demonitor(MRef, [flush]),
                            erlang:demonitor(ConductorMRef, [flush]),
                            %% Ensure ordering (only the follower needs to care
                            %% because it may be joining an in progress operation
                            case Ordered of
                                true ->
                                    %% Run again we have to wait for the current operation
                                    %% the next one with be synchronized
                                    do_run(Function, Arguments, Config, false);
                                false ->
                                    %% Error was encapsulated by the conductor lets unpack it so we behave
                                    %% as if we executed it
                                    case Result0 of
                                        %% Return the result
                                        {success, Result1} ->
                                            Result1;
                                        {value, Val} ->
                                            Val;
                                        {Class, Reason, Stacktrace} ->
                                            erlang:raise(Class, Reason, Stacktrace)
                                    end
                            end;
                        {'DOWN', MRef, _, _, Reason} ->
                            %% Coordinator died clean up and exit with the same reason
                            erlang:demonitor(MRef, [flush]),
                            erlang:demonitor(ConductorMRef, [flush]),
                            erlang:exit(Reason);
                        {'DOWN', ConductorMRef, _, _, Reason} ->
                            %% Conductor died clean up and exit
                            erlang:demonitor(MRef, [flush]),
                            erlang:demonitor(ConductorMRef, [flush]),
                            erlang:exit({conductor_died, Reason})
                    after FollowerWaitForConductorTimeout ->
                        erlang:demonitor(MRef, [flush]),
                        erlang:exit(timeout)
                    end;
                {MRef, conductor} ->
                    %%  We are assigned the conductor role by the coordinator
                    Result0 =
                        try
                            %% Run the real function specified by the user and encapsulate it
                            %% in a success tuple
                            {success, apply(Function, Arguments)}
                        catch
                            Class0:Reason0:Stacktrace0 ->
                                {Class0, Reason0, Stacktrace0}
                        end,

                    %% Let the coordinator know that we are done the work so it can stop
                    %% accumulating followers for this pier and send use the list.
                    %% We are sending it a reply to reference to call us back with the details.
                    erlang:send(CoordinatorName, {'$harbor_work_completed', {erlang:self(), MRef}}),

                    receive
                        {MRef, {followers, Followers}} ->
                            %% We have the list of followers from the coordinator now we can fan out
                            %% the result and stop monitoring the coordinator since we are now undocked
                            %% from the harbor
                            erlang:demonitor(MRef, [flush]),
                            lists:foreach(fun(Follower) -> gen:reply(Follower, Result0) end,
                                          Followers);
                        {'DOWN', MRef, _, _, Reason1} ->
                            %% Coordinator died clean up and exit
                            erlang:demonitor(MRef, [flush]),
                            erlang:exit(Reason1)
                    after ConductorWaitForCoordinatorFollowersTimeout ->
                        erlang:demonitor(MRef, [flush]),
                        erlang:exit(timeout)
                    end,

                    %% Error was encapsulated by the conductor lets unpack it so we behave
                    %% as if we executed it
                    case Result0 of
                        %% Return the result
                        {success, Result1} ->
                            Result1;
                        {value, Val} ->
                            Val;
                        {Class2, Reason2, Stacktrace2} ->
                            erlang:raise(Class2, Reason2, Stacktrace2)
                    end;
                {'DOWN', MRef, _, _, Reason} ->
                    %% Coordinator died clean up and exit
                    erlang:demonitor(MRef, [flush]),
                    erlang:exit(Reason)
            after AssignRoleTimeout ->
                erlang:demonitor(MRef, [flush]),
                erlang:exit(timeout)
            end;
        full ->
            %% We can't dock into the harbor the backlog is full, there is no
            %% space available in the buffer. Emit the user chosen error result
            erlang:demonitor(MRef, [flush]),
            case ErrorType of
                throw ->
                    erlang:throw(ErrorValue);
                error ->
                    erlang:error(ErrorValue);
                exit ->
                    erlang:exit(ErrorValue);
                value ->
                    ErrorValue
            end
    end.

%% @doc Run a function using an MFA with the config using a harbor instance.
%%
%% @end
-spec run(MFA :: mfa(), Config :: config()) -> any() | no_return().
run({Module, Function, Arguments}, Config) ->
    F = fun(Args) -> erlang:apply(Module, Function, Args) end,
    run(F, Arguments, Config).

%% @doc From the harbor name return the derived pobox name.
%%
%% The pobox is a named process and to talk to it directly
%% some operations require its name.
%%
%% @end
-spec pobox_name(Name :: atom()) -> atom().
pobox_name(Name) ->
    erlang:binary_to_atom(<<(erlang:atom_to_binary(Name))/binary, "_pobox">>).

%% @doc From the harbor name return the derived coordinator name.
%%
%% The coordinator is a named process and to talk to it directly
%% some operations require its name.
%%
%% @end
-spec coordinator_name(Name :: atom()) -> atom().
coordinator_name(Name) ->
    erlang:binary_to_atom(<<(erlang:atom_to_binary(Name))/binary, "_coordinator">>).
