-module(eharbor_coordinator).

-behavior(gen_server).

-export([start_link/1, pause/1, pause/2, get_stats/1]).
-export([init/1, handle_info/2, handle_cast/2, handle_call/3]).

-define(PAUSE_DEFAULT_MS, 100).

-record(state,
        {name,
         dedup,
         ordered,
         backlog,
         %% How many unique tasks
         piers,
         %% Breakwater_limit total number of processes waiting out of buffer
         breakwater_limit,
         breakwater_usage = 0,
         piers_in_use = 0,
         active_work_registry = #{},
         conductor_registry = #{},
         conductor_monitors = #{},
         pobox_pid = nil,
         notify_ready = true,
         total_messages = 0,
         dropped_messages = 0}).

%% API functions
-spec start_link(eharbor:incomplete_config()) -> {ok, pid()}.
start_link(IncompleteConfig) ->
    Config = #{name := Name} = eharbor:merge_with_defaults(IncompleteConfig),

    gen_server:start_link({local, eharbor:coordinator_name(Name)}, ?MODULE, Config, []).

%% For testing only simulate a busy coordinator
pause(Name) ->
    pause(Name, ?PAUSE_DEFAULT_MS).

pause(Name, Sleep) ->
    gen_server:cast(Name, {pause, Sleep}).

%% Get messages vs dropped message count since the start.
get_stats(Name) ->
    gen_server:call(Name, get_stats).

%% gen_server callbacks
init(_Config =
         #{name := Name,
           dedup := Dedup,
           ordered := Ordered,
           piers := Piers,
           breakwater_limit := BreakwaterLimit,
           backlog := Backlog}) ->
    %% Start the pobox load shedding buffer
    {ok, PoboxPid} =
        pobox:start_link({local, eharbor:pobox_name(Name)}, erlang:self(), Backlog, keep_old),
    %% Enter notify mode (tells pobox we are ready to handle new messages)
    ok = pobox:notify(PoboxPid),
    {ok,
     #state{name = Name,
            dedup = Dedup,
            ordered = Ordered,
            piers = Piers,
            breakwater_limit = BreakwaterLimit,
            backlog = Backlog,
            pobox_pid = PoboxPid}}.

handle_call(get_stats,
            _From,
            State = #state{total_messages = TotalMessages, dropped_messages = DroppedMessages}) ->
    {reply,
     #{total_messages => TotalMessages, dropped_messages => DroppedMessages},
     handle_notify(State)};
handle_call(_Msg, _From, State) ->
    {reply, ok, handle_notify(State)}.

handle_cast({pause, Sleep}, State) ->
    timer:sleep(Sleep),
    {noreply, handle_notify(State)};
handle_cast(_Msg, State) ->
    {noreply, handle_notify(State)}.

handle_info({mail, PoboxPid, new_data},
            State =
                #state{pobox_pid = PoboxPid,
                       piers = Piers,
                       piers_in_use = PiersInUse,
                       breakwater_usage = BreakwaterUsage,
                       breakwater_limit = BreakwaterLimit}) ->
    %% New data is available in our buffer determine the maximum we can accept without
    %% going over our limits and switch the pobox to active mode to get a batch.
    AvailableSlots = erlang:min(Piers - PiersInUse, BreakwaterLimit - BreakwaterUsage),
    pobox:active(PoboxPid,
                 fun (_Msg, Count) when Count =:= AvailableSlots ->
                         skip;
                     (Msg, Count) ->
                         {{ok, Msg}, Count + 1}
                 end,
                 0),
    {noreply, State};
handle_info({mail, PoboxPid, Messages, Count, DropCount},
            State =
                #state{pobox_pid = PoboxPid,
                       piers_in_use = PiersInUse,
                       breakwater_usage = BreakwaterUsage,
                       active_work_registry = ActiveWorkRegistry0,
                       conductor_registry = ConductorRegistry0,
                       conductor_monitors = ConductorMonitors0,
                       dedup = Dedup,
                       total_messages = TotalMessages,
                       dropped_messages = DroppedMessages}) ->
    ArgumentGroups =
        case Dedup of
            true ->
                group_by_key([{Args, From} || {'$habor_dock', From, Args} <- Messages]);
            false ->
                maps:from_list([{Args, [From]} || {'$habor_dock', From, Args} <- Messages])
        end,

    ArgumentGroupsKeys = maps:keys(ArgumentGroups),

    {OldArgsList, NewArgsList} =
        lists:partition(fun(ArgumentGroup) -> maps:is_key(ArgumentGroup, ActiveWorkRegistry0) end,
                        ArgumentGroupsKeys),

    [begin
         {ConductorPid, _MRef} =
             erlang:hd(
                 maps:get(OldArgs, ActiveWorkRegistry0)),
         FollowerFroms = maps:get(OldArgs, ArgumentGroups),
         lists:foreach(fun(FollowerFrom) -> gen:reply(FollowerFrom, {follower, ConductorPid}) end,
                       FollowerFroms)
     end
     || OldArgs <- OldArgsList],

    NewConductorMrefs =
        lists:foldl(fun(NewArgs, Acc) ->
                       ConductorFrom = hd(maps:get(NewArgs, ArgumentGroups)),
                       {ConductorPid, _MRef} = ConductorFrom,
                       ConductorMRef = erlang:monitor(process, ConductorPid),
                       Acc#{ConductorPid => {ConductorMRef, ConductorFrom}}
                    end,
                    #{},
                    NewArgsList),

    NewConductors =
        maps:from_list(
            lists:map(fun(NewArgs) ->
                         ConductorFrom = hd(maps:get(NewArgs, ArgumentGroups)),
                         {ConductorPid, _Mref} = ConductorFrom,
                         FollowerFroms = tl(maps:get(NewArgs, ArgumentGroups)),
                         gen:reply(ConductorFrom, conductor),
                         lists:foreach(fun(FollowerFrom) ->
                                          gen:reply(FollowerFrom, {follower, ConductorPid})
                                       end,
                                       FollowerFroms),
                         {ConductorFrom, NewArgs}
                      end,
                      NewArgsList)),
    ConductorRegistry1 = maps:merge(ConductorRegistry0, NewConductors),
    ActiveWorkRegistry1 =
        maps:merge_with(fun(_K, V1, V2) -> V1 ++ V2 end, ActiveWorkRegistry0, ArgumentGroups),

    ConductorMonitors1 = maps:merge(ConductorMonitors0, NewConductorMrefs),

    {noreply,
     handle_notify(State#state{active_work_registry = ActiveWorkRegistry1,
                               conductor_registry = ConductorRegistry1,
                               conductor_monitors = ConductorMonitors1,
                               piers_in_use = PiersInUse + erlang:length(NewArgsList),
                               breakwater_usage = BreakwaterUsage + Count,
                               notify_ready = true,
                               total_messages = TotalMessages + Count,
                               dropped_messages = DroppedMessages + DropCount})};
handle_info({'$harbor_work_completed', ConductorFrom},
            State =
                #state{active_work_registry = ActiveWorkRegistry0,
                       conductor_registry = ConductorRegistry0,
                       conductor_monitors = ConductorMonitors0,
                       breakwater_usage = BreakwaterUsage,
                       piers_in_use = PiersInUse}) ->
    ConductorArgs = maps:get(ConductorFrom, ConductorRegistry0),
    {[ConductorFrom | Followers] = BreakwaterProcs, ActiveWorkRegistry1} =
        maps:take(ConductorArgs, ActiveWorkRegistry0),
    gen:reply(ConductorFrom, {followers, Followers}),
    {ConductorArgs, ConductorRegistry1} = maps:take(ConductorFrom, ConductorRegistry0),
    {ConductorPid, _ConductorMRef} = ConductorFrom,
    {{ConductorMRef, _ConductorFrom}, ConductorMonitors1} =
        maps:take(ConductorPid, ConductorMonitors0),
    erlang:demonitor(ConductorMRef, [flush]),
    {noreply,
     handle_notify(State#state{active_work_registry = ActiveWorkRegistry1,
                               conductor_registry = ConductorRegistry1,
                               conductor_monitors = ConductorMonitors1,
                               breakwater_usage = BreakwaterUsage - erlang:length(BreakwaterProcs),
                               piers_in_use = PiersInUse - 1})};
handle_info({'DOWN', _ConductorMRef, process, ConductorPid, _Reason},
            State =
                #state{active_work_registry = ActiveWorkRegistry0,
                       conductor_registry = ConductorRegistry0,
                       conductor_monitors = ConductorMonitors0,
                       breakwater_usage = BreakwaterUsage,
                       piers_in_use = PiersInUse}) ->
    {{ConductorMRef, ConductorFrom}, ConductorMonitors1} =
        maps:take(ConductorPid, ConductorMonitors0),
    {Args, ConductorRegistry1} = maps:take(ConductorFrom, ConductorRegistry0),
    {Froms, ActiveWorkRegistry1} = maps:take(Args, ActiveWorkRegistry0),
    erlang:demonitor(ConductorMRef, [flush]),
    {noreply,
     handle_notify(State#state{active_work_registry = ActiveWorkRegistry1,
                               conductor_registry = ConductorRegistry1,
                               conductor_monitors = ConductorMonitors1,
                               breakwater_usage = BreakwaterUsage - erlang:length(Froms),
                               piers_in_use = PiersInUse - 1})};
handle_info(ping, State) ->
    {noreply, handle_notify(State)};
handle_info(_Msg, State) ->
    {noreply, handle_notify(State)}.

handle_notify(State =
                  #state{pobox_pid = PoboxPid,
                         breakwater_usage = BreakwaterUsage,
                         breakwater_limit = BreakwaterLimit,
                         piers_in_use = PiersInUse,
                         piers = Piers,
                         notify_ready = true}) ->
    {message_queue_len, QueueLen} =
        erlang:process_info(
            erlang:self(), message_queue_len),
    case QueueLen =:= 0 andalso BreakwaterUsage < BreakwaterLimit andalso PiersInUse < Piers
    of
        true ->
            pobox:notify(PoboxPid),
            State#state{notify_ready = false};
        false ->
            State
    end;
handle_notify(State) ->
    State.

group_by_key(KVs) ->
    lists:foldl(fun({K, V}, Acc) ->
                   case maps:is_key(K, Acc) of
                       true ->
                           Value = maps:get(K, Acc),
                           maps:put(K, [V | Value], Acc);
                       false ->
                           maps:put(K, [V], Acc)
                   end
                end,
                #{},
                KVs).
