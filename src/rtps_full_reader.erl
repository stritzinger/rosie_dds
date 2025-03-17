% Reliable StateFull RTPS reader with WriterProxies, receives heartbits and sends acknacks
-module(rtps_full_reader).

-export([
    start_link/1,
    matched_writer_add/2,
    matched_writer_remove/2,
    update_matched_writers/2,
    receive_data/2,
    receive_gap/2,
    get_cache/1,
    receive_heartbeat/2
]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-include_lib("kernel/include/logger.hrl").
-include("../include/rtps_structure.hrl").

-record(state,
        {participant = #participant{},
         entity = #endPoint{},
         history_cache,
         writer_proxies = [],
         heartbeatResponseDelay = 20, %default should be 500
         heartbeatSuppressionDuration = 0,
         acknack_count = 0}).

%API
start_link({Participant, ReaderConfig}) ->
    State = #state{participant = Participant, entity = ReaderConfig},
    gen_server:start_link(?MODULE, State, []).

get_cache(Name) ->
    [Pid | _] = pg:get_members(Name),
    gen_server:call(Pid, get_cache).

receive_heartbeat(Pid, HB) when is_pid(Pid) ->
    gen_server:cast(Pid, {receive_heartbeat, HB});
receive_heartbeat(Name, HB) ->
    [Pid | _] = pg:get_members(Name),
    gen_server:cast(Pid, {receive_heartbeat, HB}).

update_matched_writers(Name, Proxies) ->
    [Pid | _] = pg:get_members(Name),
    gen_server:cast(Pid, {update_matched_writers, Proxies}).

matched_writer_add(Name, W) ->
    [Pid | _] = pg:get_members(Name),
    gen_server:cast(Pid, {matched_writer_add, W}).

matched_writer_remove(Name, W) ->
    [Pid | _] = pg:get_members(Name),
    gen_server:cast(Pid, {matched_writer_remove, W}).

receive_data(Pid, Data) when is_pid(Pid) ->
    gen_server:cast(Pid, {receive_data, Data});
receive_data(Name, Data) ->
    [Pid | _] = pg:get_members(Name),
    gen_server:cast(Pid, {receive_data, Data}).

receive_gap(Name, Gap) ->
    [Pid | _] = pg:get_members(Name),
    gen_server:cast(Pid, {receive_gap, Gap}).

% callbacks
init(#state{entity = E} = State) ->
    %io:format("~p.erl STARTED!\n",[?MODULE]),
    pg:join(E#endPoint.guid, self()),
    pg:join(rtps_readers, self()),
    {ok, State#state{history_cache = {cache_of, E#endPoint.guid}}}.

handle_call(get_cache, _, State) ->
    {reply, State#state.history_cache, State};
handle_call(_, _, State) ->
    {reply, ok, State}.

handle_cast({receive_data, {_, _, #data_frag{}} = DataFrag}, State) ->
    {noreply, h_receive_data_frag(DataFrag, State)};
handle_cast({receive_data, {_, _, Payload} = Data}, State)
when is_binary(Payload); is_record(Payload, sedp_disc_endpoint_data) ->
    {noreply, h_receive_data(Data, State)};
handle_cast({receive_gap, Gap}, State) ->
    {noreply, h_receive_gap(Gap, State)};
handle_cast({receive_heartbeat, HB}, State) ->
    {noreply, h_receive_heartbeat(HB, State)};
handle_cast({update_matched_writers, Proxies}, S) ->
    {noreply, h_update_matched_writers(Proxies, S)};
handle_cast({matched_writer_add, Proxy}, S) ->
    {noreply, h_matched_writer_add(Proxy, S)};
handle_cast({matched_writer_remove, R}, S) ->
    {noreply, h_matched_writer_remove(R, S)};
handle_cast(Cast, State) ->
    error({unexpected_cast, Cast}),
    {noreply, State}.

handle_info({send_acknack_if_needed, {WGUID, FF}}, State) ->
    {noreply, h_send_acknack_if_needed(WGUID, FF, State)};
handle_info(_, State) ->
    {noreply, State}.

%helpers
data_to_cache_change({Writer, SN, Data}) ->
    #cacheChange{writerGuid = Writer,
                 sequenceNumber = SN,
                 data = Data}.

h_update_matched_writers(Proxies, #state{writer_proxies = WP} = S) ->
    %io:format("Updating with proxy: ~p\n",[Proxies]),
    NewProxies =
        [Proxy
         || #writer_proxy{guid = GUID} = Proxy <- Proxies,
            not lists:member(GUID, [G || #writer_proxy{guid = G} <- WP])],
    S#state{writer_proxies = WP ++ NewProxies}.

h_matched_writer_add(Proxy, #state{writer_proxies = WP} = S) ->
    S#state{writer_proxies = [Proxy | WP]}.

h_matched_writer_remove(Guid, #state{writer_proxies = WP} = S) ->
    S#state{writer_proxies = [P || #writer_proxy{guid = G} = P <- WP, G /= Guid]}.

missing_changes_update(Changes, Min, Max) ->
    NewChanges =
        maps:from_list([
            {SN, #change_from_writer{status = missing}}
            || SN <- lists:seq(Min, Max), not maps:is_key(SN, Changes)]),
    maps:merge(Changes, NewChanges).

gen_change_if_not_there(LastSN, ToBeMarked) ->
    case lists:member(LastSN, [SN || {SN, _} <- ToBeMarked]) of
        true -> [];
        false -> [{LastSN, #change_from_writer{}}]
    end.

lost_changes_update(Changes, FirstSN, LastSN) ->
    {ToBeMarked, Others} = lists:partition(
        fun({SN, #change_from_writer{status = S}}) ->
            ((S == unknown) or (S == missing)) and (SN < FirstSN)
        end,
        maps:to_list(Changes)),
    LastLostChange = case LastSN < FirstSN of
        true -> gen_change_if_not_there(LastSN, ToBeMarked);
        false -> []
    end,
    maps:from_list(
        Others ++
        [{SN, C#change_from_writer{status = lost}} || {SN,C} <- ToBeMarked ++ LastLostChange]).

manage_heartbeat_for_writer(#heartbeat{writerGUID = WGUID,
                                       final_flag = FF,
                                       min_sn = Min,
                                       max_sn = Max},
                            #writer_proxy{changes = Changes} = W,
                            #state{writer_proxies = WP, heartbeatResponseDelay = Delay} = S) ->
    %io:format("~p\n",[WGUID]),
    Others = [P || #writer_proxy{guid = G} = P <- WP, G /= WGUID],
    NewChanges = missing_changes_update(Changes, Min, Max),
    NewChanges2 = lost_changes_update(NewChanges, Min, Max),
    erlang:send_after(Delay, self(), {send_acknack_if_needed, {WGUID, FF}}),
    S#state{writer_proxies = Others ++ [W#writer_proxy{changes = NewChanges2}]}.

h_receive_heartbeat(#heartbeat{writerGUID = WGUID,
                               min_sn = _Min,
                               max_sn = _Max} =
                        HB,
                    #state{writer_proxies = Proxies} = S) ->
    case [WP || #writer_proxy{guid = WPG} = WP <- Proxies, WPG == WGUID] of
        [] ->
            S;
        [W | _] ->
            manage_heartbeat_for_writer(HB, W, S)
    end.

filter_missing_sn(Changes) ->
    [SN || {SN, #change_from_writer{status = S}}
        <- maps:to_list(Changes), S == missing].

available_change_max(Changes) when Changes == #{} ->
    0;
available_change_max(Changes) ->
    lists:max(maps:keys(Changes)).

% 0 means flag not set, an acknowledgment must be sent
h_send_acknack_if_needed(WGUID, 0, #state{writer_proxies = WP} = S) ->
    case [P || #writer_proxy{guid = G} = P <- WP, G == WGUID] of
        [P | _] ->
            case filter_missing_sn(P#writer_proxy.changes) of
                [] ->
                    Missing = available_change_max(P#writer_proxy.changes) + 1;
                List ->
                    Missing = List
            end,
            send_acknack(WGUID, Missing, S);
        [] ->
            S
    end;
% 1 means flag set, i acknoledge only if i know to miss some data
h_send_acknack_if_needed(WGUID, 1, #state{writer_proxies = WP} = S) ->
    case [P || #writer_proxy{guid = G} = P <- WP, G == WGUID] of
        [P | _] ->
            case filter_missing_sn(P#writer_proxy.changes) of
                [] ->
                    S;
                List ->
                    send_acknack(WGUID, List, S)
            end;
        [] ->
            S
    end.

send_acknack(WGUID,
             Missing,
             #state{entity = #endPoint{guid = RGUID},
                    writer_proxies = WP,
                    acknack_count = C} =
                 S) ->
    %io:format("Sending an ack nack for ~p, to ~p\n", [Missing, WGUID#guId.entityId]),
    %io:format("Proxies are: ~p\n",[[ P || #writer_proxy{guid=G}=P <- WP, G == WGUID]]),
    [[L | _] | _] =
        [U_List
         || #writer_proxy{guid = GUID, unicastLocatorList = U_List} <- WP, GUID == WGUID],
    ACKNACK =
        #acknack{writerGUID = WGUID,
                 readerGUID = RGUID,
                 final_flag = 1,
                 sn_range = Missing,
                 count = C},
    Datagram =
        rtps_messages:build_message(RGUID#guId.prefix,
                                    [rtps_messages:serialize_info_dst(WGUID#guId.prefix),
                                     rtps_messages:serialize_acknack(ACKNACK)]),
    [G | _] = pg:get_members(rtps_gateway),
    rtps_gateway:send(G, {Datagram, {L#locator.ip, L#locator.port}}),
    S#state{acknack_count = C + 1}.

h_receive_data({WriterID, SN, Data},
               #state{history_cache = Cache, writer_proxies = WP} = State) ->
    {Match, Proxies} =
        lists:partition(fun(#writer_proxy{guid = ID}) when ID == WriterID-> true;
                           (_) -> false end,
                        WP),
    case Match of
        [] ->
            State;
        [Proxy] ->
            rtps_history_cache:add_change(Cache, data_to_cache_change({WriterID, SN, Data})),
            Change = maps:get(SN,
                             Proxy#writer_proxy.changes,
                            #change_from_writer{status = missing}),
            ChangeReceived = Change#change_from_writer{status = received},
            NewChanges = maps:put(SN, ChangeReceived, Proxy#writer_proxy.changes),
            State#state{
                writer_proxies = [Proxy#writer_proxy{changes = NewChanges} | Proxies]};
        _ ->
            error({unimplemented, multiple_writer_proxies})
    end.

h_receive_data_frag({WriterID, SN, DataFrag},
               #state{history_cache = Cache, writer_proxies = WP} = State) ->
    #data_frag{
        start_num = FragmentStartingNum,
        count = FragmentsInSubmessage,
        fragment_size = FragmentSize,
        sample_size = SampleSize,
        fragments = Fragments
     } = DataFrag,
    {Match, Proxies} =
        lists:partition(fun(#writer_proxy{guid = ID}) when ID == WriterID -> true;
                           (_) -> false end,
                        WP),
    case Match of
        [] ->
            State;
        [Proxy] ->
            NewProxy = add_fragments_to_proxy(Proxy, DataFrag),
            State#state{writer_proxies = [NewProxy|Proxies]};
        _ ->
            error({unimplemented, multiple_writer_proxies})
    end.

h_receive_gap(#gap{writerGUID = WriterID, sn_set = SET},
               #state{history_cache = _Cache, writer_proxies = WP} = State) ->
    {Match, Proxies} =
            lists:partition(fun(#writer_proxy{guid = ID}) when ID == WriterID -> true;
                               (_) -> false end,
                            WP),
    case Match of
        [] ->
            State;
        [Proxy] ->
            %io:format("GAP processing...for numbers ~p\n", [SET]),
            %DEBUG_SN_STATES = [ {SN,S} || #change_from_writer{change_key = {_,SN}, status = S} <- Selected#writer_proxy.changes],
            %io:format("Current changes state ~p\n", [DEBUG_SN_STATES]),
            AddedChanges = lists:foldl(
                fun (SN, AllChanges) ->
                    Change = maps:get(SN,
                                     AllChanges
                                     #change_from_writer{status = missing}),
                    maps:put(SN, Change, AllChanges)
                end,
                Proxy#writer_proxy.changes,
                SET),

            AddedAndMarked = maps:map(fun({SN, C}) ->
                                            case lists:member(SN, SET) of
                                                true -> {SN, C#change_from_writer{status = received}};
                                                false -> {SN, C}
                                            end
                                        end, AddedChanges),
            State#state{writer_proxies = [Proxy#writer_proxy{changes = AddedAndMarked} | Proxies]};
        _ ->
            error({unimplemented, multiple_writer_proxies})
    end.


add_fragments_to_proxy(#writer_proxy{guid = WriterID} = P, DataFrag) ->
    P.
