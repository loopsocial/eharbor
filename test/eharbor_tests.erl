-module(eharbor_tests).

-include_lib("eunit/include/eunit.hrl").

function_call_success_test() ->
    % Define the custom configuration
    Config = (eharbor:config_defaults())#{name := ?TEST},

    % Start the coordinator
    eharbor_coordinator:start_link(Config),

    % Define a function to be called
    Fun = fun(X) -> X + 1 end,

    % Call the function
    Result = eharbor:run(Fun, [1], Config),
    % Assert the result
    ?assertEqual(2, Result),

    % Kill the coordinator
    CoordinatorName = eharbor:coordinator_name(?TEST),
    process_flag(trap_exit, true),
    Pid = whereis(CoordinatorName),
    exit(Pid, kill),

    % Receive the exit message
    receive
        {'EXIT', Pid, Reason} ->
            ?assert(Reason == killed)
    end.

function_call_error_test() ->
    % Define the custom configuration
    Config = (eharbor:config_defaults())#{name := ?TEST, backlog := 1},
    % Start the coordinator
    eharbor_coordinator:start_link(Config),
    % Define a function to be called
    Fun = fun(X) -> X + 1 end,
    eharbor_coordinator:pause(
        eharbor:coordinator_name(?TEST), timer:seconds(30)),
    % Call the function
    erlang:spawn(fun() -> eharbor:run(Fun, [1], Config) end),
    poll(fun() ->
            {Current, _Max} =
                pobox:usage(
                    eharbor:pobox_name(?TEST)),
            Current
         end,
         fun(Size) -> Size == 1 end,
         5000,
         1),
    Result = eharbor:run(Fun, [1], Config),
    % Assert the result
    ?assertEqual({error, full}, Result),
    % Kill the coordinator
    CoordinatorName = eharbor:coordinator_name(?TEST),
    Pid = whereis(CoordinatorName),
    exit(Pid, kill),
    % Receive the exit message
    receive
        {'EXIT', Pid, Reason} ->
            ?assert(Reason == killed)
    end.

piers_test() ->
    % Define the custom configuration
    Config =
        (eharbor:config_defaults())#{name := ?TEST,
                                     piers := 1,
                                     ordered := false,
                                     breakwater_limit := 20},

    % Start the coordinator
    eharbor_coordinator:start_link(Config),

    % Define a function to be called
    Fun = fun(_) ->
             StartTime = erlang:monotonic_time(millisecond),
             timer:sleep(100),
             EndTime = erlang:monotonic_time(millisecond),
             {StartTime, EndTime}
          end,

    % Call the function twice
    Parent = self(),
    Ref1 = make_ref(),
    erlang:spawn(fun() -> Parent ! {Ref1, eharbor:run(Fun, [1], Config)} end),
    Ref2 = make_ref(),
    erlang:spawn(fun() -> Parent ! {Ref2, eharbor:run(Fun, [2], Config)} end),
    FirstEndTime =
        receive
            {Ref1, {_ST1, ET1}} ->
                ET1
        end,
    SecondStartTime =
        receive
            {Ref2, {ST2, _ET2}} ->
                ST2
        end,

    % Assert that it ran after the first
    ?assert(SecondStartTime >= FirstEndTime),

    % Kill the coordinator
    CoordinatorName = eharbor:coordinator_name(?TEST),
    Pid = whereis(CoordinatorName),
    exit(Pid, kill),
    % Receive the exit message
    receive
        {'EXIT', Pid, Reason} ->
            ?assert(Reason == killed)
    end.

piers_waits_when_to_many_activities_test() ->
    % Define the custom configuration
    Config =
        (eharbor:config_defaults())#{name := ?TEST,
                                     piers := 2,
                                     ordered := false,
                                     breakwater_limit := 20},

    % Start the coordinator
    eharbor_coordinator:start_link(Config),

    % Define a function to be called
    Fun = fun(_) ->
             StartTime = erlang:monotonic_time(millisecond),
             timer:sleep(100),
             EndTime = erlang:monotonic_time(millisecond),
             {StartTime, EndTime}
          end,

    % Call the function three times
    Parent = self(),
    Ref1 = make_ref(),
    erlang:spawn(fun() -> Parent ! {Ref1, eharbor:run(Fun, [1], Config)} end),
    Ref2 = make_ref(),
    erlang:spawn(fun() -> Parent ! {Ref2, eharbor:run(Fun, [2], Config)} end),
    Ref3 = make_ref(),
    erlang:spawn(fun() -> Parent ! {Ref3, eharbor:run(Fun, [3], Config)} end),
    FirstEndTime =
        receive
            {Ref1, {_, ET1}} ->
                ET1
        end,
    {SecondStartTime, SecondEndTime} =
        receive
            {Ref2, {ST2, ET2}} ->
                {ST2, ET2}
        end,
    ThirdStartTime =
        receive
            {Ref3, {ST3, _ET3}} ->
                ST3
        end,

    % Assert that it ran concurrently
    ?assert(FirstEndTime > SecondStartTime),
    % Assert that it ran after the second function
    ?assert(SecondEndTime =< ThirdStartTime),

    % Kill the coordinator
    CoordinatorName = eharbor:coordinator_name(?TEST),
    Pid = whereis(CoordinatorName),
    exit(Pid, kill),
    % Receive the exit message
    receive
        {'EXIT', Pid, Reason} ->
            ?assert(Reason == killed)
    end.

breakwater_limit_test() ->
    % Define the custom configuration
    Config = (eharbor:config_defaults())#{name := ?TEST, breakwater_limit := 1},

    % Start the coordinator
    eharbor_coordinator:start_link(Config),

    % Define a function to be called
    Fun = fun(_) ->
             StartTime = erlang:monotonic_time(millisecond),
             timer:sleep(100),
             EndTime = erlang:monotonic_time(millisecond),
             {StartTime, EndTime}
          end,

    % Call the function twice
    Parent = self(),
    erlang:spawn(fun() -> Parent ! eharbor:run(Fun, [1], Config) end),
    FirstEndTime =
        receive
            {_, EndTime} ->
                EndTime
        end,
    {SecondStartTime, _SecondEndTime} = eharbor:run(Fun, [1], Config),

    % Assert that it ran after the first
    ?assert(FirstEndTime =< SecondStartTime),

    % Kill the coordinator
    CoordinatorName = eharbor:coordinator_name(?TEST),
    Pid = whereis(CoordinatorName),
    exit(Pid, kill),
    % Receive the exit message
    receive
        {'EXIT', Pid, Reason} ->
            ?assert(Reason == killed)
    end.

stats_test() ->
    Config =
        (eharbor:config_defaults())#{name := ?TEST,
                                     backlog := 1,
                                     ordered := false,
                                     dedup := false},
    CoordinatorName = eharbor:coordinator_name(?TEST),
    % Start the coordinator
    eharbor_coordinator:start_link(Config),
    eharbor_coordinator:pause(CoordinatorName, 100),
    erlang:spawn(fun() -> eharbor:run(fun(X) -> X + 1 end, [1], Config) end),
    erlang:spawn(fun() -> eharbor:run(fun(X) -> X + 1 end, [1], Config) end),
    erlang:spawn(fun() -> eharbor:run(fun(X) -> X + 1 end, [1], Config) end),
    timer:sleep(200),
    #{total_messages := TotalMessages, dropped_messages := DroppedMessages} =
        eharbor_coordinator:get_stats(CoordinatorName),
    ?assertEqual(TotalMessages, 1),
    ?assertEqual(DroppedMessages, 2),
    % Kill the coordinator
    Pid = whereis(CoordinatorName),
    exit(Pid, kill),
    % Receive the exit message
    receive
        {'EXIT', Pid, Reason} ->
            ?assert(Reason == killed)
    end.

dedup_test() ->
    Config =
        (eharbor:config_defaults())#{name := ?TEST,
                                     backlog := 1000,
                                     ordered := false,
                                     dedup := true},
    CoordinatorName = eharbor:coordinator_name(?TEST),
    CounterRef = counters:new(1, [atomics]),
    % Start the coordinator
    eharbor_coordinator:start_link(Config),
    eharbor_coordinator:pause(CoordinatorName, 100),
    F = fun(X) ->
           counters:add(CounterRef, 1, 1),
           X + 1
        end,
    [erlang:spawn(fun() -> eharbor:run(F, [1], Config) end) || _ <- lists:seq(1, 100)],
    timer:sleep(200),

    ?assert(counters:get(CounterRef, 1) =:= 1),

    % Kill the coordinator
    Pid = whereis(CoordinatorName),
    exit(Pid, kill),
    % Receive the exit message
    receive
        {'EXIT', Pid, Reason} ->
            ?assert(Reason == killed)
    end.

dedup_ordered_test() ->
    Config =
        (eharbor:config_defaults())#{name := ?TEST,
                                     backlog := 1000,
                                     ordered := true,
                                     dedup := true},
    CoordinatorName = eharbor:coordinator_name(?TEST),
    CounterRef = counters:new(1, [atomics]),
    % Start the coordinator
    eharbor_coordinator:start_link(Config),
    eharbor_coordinator:pause(CoordinatorName, 200),
    F = fun(_) ->
           counters:add(CounterRef, 1, 1),
           timer:sleep(100)
        end,
    [erlang:spawn(fun() -> eharbor:run(F, [1], Config) end) || _ <- lists:seq(1, 100)],
    timer:sleep(1000),

    ?assert(counters:get(CounterRef, 1) =:= 2),

    % Kill the coordinator
    Pid = whereis(CoordinatorName),
    exit(Pid, kill),
    % Receive the exit message
    receive
        {'EXIT', Pid, Reason} ->
            ?assert(Reason == killed)
    end.

conductor_fails_mid_flight_test() ->
    error_logger:tty(false),
    application:stop(logger),
    try
        Config =
            (eharbor:config_defaults())#{name := ?TEST,
                                         backlog := 1000,
                                         ordered := true,
                                         dedup := true},
        CoordinatorName = eharbor:coordinator_name(?TEST),
        % Start the coordinator
        eharbor_coordinator:start_link(Config),
        eharbor_coordinator:pause(CoordinatorName, 200),

        F = fun(_) ->
               timer:sleep(50),
               erlang:error(boom)
            end,
        [erlang:spawn(fun() -> eharbor:run(F, [1], Config) end) || _ <- lists:seq(1, 6)],

        ?assert(case catch eharbor:run(F, [1], Config) of
                    {'EXIT', {boom, _}} ->
                        true;
                    other ->
                        other
                end),

        % Kill the coordinator
        Pid = whereis(CoordinatorName),
        exit(Pid, kill),
        % Receive the exit message
        receive
            {'EXIT', Pid, Reason} ->
                ?assert(Reason == killed)
        end
    after
        application:start(logger),
        error_logger:tty(true)
    end.

poll(Callback, Condition, Timeout, Interval) ->
    StartTime = erlang:monotonic_time(millisecond),
    poll_interval(Callback, Condition, Timeout, Interval, StartTime).

poll_interval(Callback, Condition, Timeout, Interval, StartTime) ->
    Result = Callback(),
    case Condition(Result) of
        true ->
            true;
        false ->
            CurrentTime = erlang:monotonic_time(millisecond),
            ElapsedTime = CurrentTime - StartTime,
            case ElapsedTime >= Timeout of
                true ->
                    false;
                false ->
                    timer:sleep(Interval),
                    poll_interval(Callback, Condition, Timeout, Interval, StartTime)
            end
    end.
