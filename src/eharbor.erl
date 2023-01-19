%%%-------------------------------------------------------------------
%% @doc eharbor
%% @end
%%%-------------------------------------------------------------------

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

config_defaults() ->
    ?CONFIG_DEFAULTS.

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

-spec run(mfa(), config()) -> any() | no_return().
run({Module, Function, Arguments}, Config) ->
    F = fun(Args) -> erlang:apply(Module, Function, Args) end,
    run(F, Arguments, Config).

pobox_name(Name) ->
    erlang:binary_to_atom(<<(erlang:atom_to_binary(Name))/binary, "_pobox">>).

coordinator_name(Name) ->
    erlang:binary_to_atom(<<(erlang:atom_to_binary(Name))/binary, "_coordinator">>).
