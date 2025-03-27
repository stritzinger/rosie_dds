% Realiable statefull writer, it manages Reader-proxies sends heatbeats and receives acknacks
-module(rtps_full_writer).

-export([
    start_link/1,
    on_change_available/2,
    on_change_removed/2, new_change/2,
    get_matched_readers/1,
    get_cache/1,
    update_matched_readers/2,
    matched_reader_add/2,
    matched_reader_remove/2,
    is_acked_by_all/2,
    receive_acknack/2,
    flush_all_changes/1
]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-include("../include/rtps_structure.hrl").
-include("../include/rtps_constants.hrl").

-define(DEFAULT_WRITE_PERIOD, 1000).
-define(DEFAULT_HEARTBEAT_PERIOD, 1000).
-define(DEFAULT_NACK_RESPONCE_DELAY, 200).

-record(state,
        {participant = #participant{},
         entity = #endPoint{},
         datawrite_period = ?DEFAULT_WRITE_PERIOD div 10, % default at 1000
         heatbeat_period = ?DEFAULT_HEARTBEAT_PERIOD div 10, % default at 1000
         nackResponseDelay = ?DEFAULT_NACK_RESPONCE_DELAY div 10, % default at 200
         heatbeat_count = 1,
         nackSuppressionDuration = 0,
         push_mode = true,
         history_cache,
         reader_proxies = [],
         last_sequence_number = 0}).

%API
start_link({Participant, WriterConfig}) ->
    gen_server:start_link(?MODULE, {Participant, WriterConfig}, []).

new_change(Name, Data) ->
    [Pid | _] = pg:get_members(Name),
    gen_server:call(Pid, {new_change, Data}).

on_change_available(Name, ChangeKey) ->
    [Pid | _] = pg:get_members(Name),
    gen_server:cast(Pid, {on_change_available, ChangeKey}).

on_change_removed(Name, ChangeKey) ->
    [Pid | _] = pg:get_members(Name),
    gen_server:cast(Pid, {on_change_removed, ChangeKey}).

%Adds new locators if missing, removes old locators not specified in the call.
update_matched_readers(Name, R) ->
    [Pid | _] = pg:get_members(Name),
    gen_server:cast(Pid, {update_matched_readers, R}).

get_matched_readers(Name) ->
    [Pid | _] = pg:get_members(Name),
    gen_server:call(Pid, get_matched_readers).

matched_reader_add(Name, R) ->
    [Pid | _] = pg:get_members(Name),
    gen_server:cast(Pid, {matched_reader_add, R}).

matched_reader_remove(Name, R) ->
    [Pid | _] = pg:get_members(Name),
    gen_server:cast(Pid, {matched_reader_remove, R}).

is_acked_by_all(Name,ChangeKey) ->
    [Pid | _] = pg:get_members(Name),
    gen_server:call(Pid, {is_acked_by_all,ChangeKey}).

receive_acknack(Name, Acknack) ->
    [Pid | _] = pg:get_members(Name),
    gen_server:cast(Pid, {receive_acknack, Acknack}).

get_cache(Name) ->
    [Pid | _] = pg:get_members(Name),
    gen_server:call(Pid, get_cache).

flush_all_changes(Name) ->
    [Pid | _] = pg:get_members(Name),
    gen_server:call(Pid, flush_all_changes).

% callbacks
init({Participant, #endPoint{guid = GUID} = WriterConfig}) ->
    pg:join(GUID, self()),
    %io:format("~p.erl STARTED!\n",[?MODULE]),
    State =
        #state{participant = Participant,
               entity = WriterConfig,
               history_cache = {cache_of, GUID}},
    rtps_history_cache:set_listener({cache_of, GUID}, {?MODULE, GUID}),

    erlang:send_after(10, self(), heartbeat_loop),
    erlang:send_after(10, self(), write_loop),
    {ok, State}.

handle_call({new_change, Data}, _, State) ->
    {Change, NewState} = h_new_change(Data, State),
    {reply, Change, NewState};
handle_call(get_cache, _, State) ->
    {reply, State#state.history_cache, State};
handle_call({is_acked_by_all,ChangeKey}, _, State) ->
    {reply, h_is_acked_by_all(ChangeKey,State), State};
handle_call(get_matched_readers, _, State) ->
    {reply,  State#state.reader_proxies, State};
handle_call(flush_all_changes, _, State) ->
    {reply, h_flush_all_changes(State), State}.

handle_cast({on_change_available, ChangeKey}, S) ->
    {noreply, h_on_change_available(ChangeKey, S)};
handle_cast({on_change_removed, ChangeKey}, S) ->
    {noreply, h_on_change_removed(ChangeKey, S)};
handle_cast({update_matched_readers, Proxies}, State) ->
    {noreply, h_update_matched_readers(Proxies, State)};
handle_cast({matched_reader_add, Proxy}, State) ->
    {noreply, h_matched_reader_add(Proxy, State)};
handle_cast({matched_reader_remove, Guid}, State) ->
    {noreply, h_matched_reader_remove(Guid, State)};
handle_cast({receive_acknack, Acknack}, State) ->
    {noreply, h_receive_acknack(Acknack, State)}.

handle_info(heartbeat_loop, State) ->
    {noreply, heartbeat_loop(State)};
handle_info(write_loop, State) ->
    {noreply, write_loop(State)}.

%callback helpers

all_changes_are_acknowledged(ReaderChanges) ->
    lists:all(fun
            (#change_for_reader{status = acknowledged}) -> true;
            (_) -> false
        end,
        maps:values(ReaderChanges)).

send_heartbeat_to_reader(GuidPrefix,
                          HB,
                          #reader_proxy{guid = ReaderGUID,
                                         changes_for_reader = C4R,
                                         unicastLocatorList = [L | _]}) ->
    case all_changes_are_acknowledged(C4R) of
        true -> ok;
        false ->
            [G | _] = pg:get_members(rtps_gateway),
            NewHB = HB#heartbeat{readerGUID = ReaderGUID},
            SUB_MSG_LIST = [rtps_messages:serialize_heatbeat(NewHB)],
            Datagram = rtps_messages:build_message(GuidPrefix, SUB_MSG_LIST),
            rtps_gateway:send(G, {Datagram, {L#locator.ip, L#locator.port}})
    end.

build_heartbeat(GUID, Cache, Count) ->
    MinSN = rtps_history_cache:get_min_seq_num(Cache),
    MaxSN = rtps_history_cache:get_max_seq_num(Cache),
    #heartbeat{writerGUID = GUID,
               min_sn = MinSN,
               max_sn = MaxSN,
               count = Count,
               final_flag = 0,
               readerGUID = ?GUID_UNKNOWN}.

% Must send the first HB with min 1 and max 0 to allow first MSG to be considered,
% This triggers an acknack response with final_flag=1 which means the next data is really requested
% This is already done by the rtps_history_cache:get_min_seq_num() and rtps_history_cache:get_max_seq_num()
send_heartbeat(#state{entity = #endPoint{guid = GUID},
                      history_cache = C,
                      heatbeat_count = Count,
                      reader_proxies = RP}) ->
    HB = build_heartbeat(GUID, C, Count),
    [send_heartbeat_to_reader(GUID#guId.prefix, HB, Proxy) || Proxy <- RP].

heartbeat_loop(#state{heatbeat_period = HP, heatbeat_count = C} = S) ->
    send_heartbeat(S),
    erlang:send_after(HP, self(), heartbeat_loop),
    S#state{heatbeat_count = C + 1}.

send_selected_changes([], _, _, #reader_proxy{changes_for_reader = CR}) ->
    CR;
send_selected_changes(RequestedSN,
                      Guid,
                      HC,
                      #reader_proxy{guid = #guId{entityId = RID},
                                    unicastLocatorList = [L | _],
                                    changes_for_reader = CR}) ->
    ToSend = [rtps_history_cache:get_change(HC, {Guid, SN}) || SN <- RequestedSN],
    ValidToSend = [C || C <- ToSend, C /= not_found],
    [G | _] = pg:get_members(rtps_gateway),
    SUB_MSG =
        [rtps_messages:serialize_info_timestamp()]
        ++ [rtps_messages:serialize_data(RID, C) || C <- ValidToSend],
    Msg = rtps_messages:build_message(Guid#guId.prefix, SUB_MSG),
    rtps_gateway:send(G, {Msg, {L#locator.ip, L#locator.port}}),

    % mark all sent requests as "unacknowledged" (skipping the "UNDERWAY" status) just for simplicity
    maps:map(fun(Key, Change) ->
                case lists:member(Key, RequestedSN) of
                    true ->
                        Change#change_for_reader{status = unacknowledged};
                    false ->
                        Change
                end
            end,
            CR).

rec_send_changes(_, _, _, [], Sent) ->
    Sent;
rec_send_changes(Filter,
             Guid,
             HC,
             [#reader_proxy{guid = #guId{entityId = _RID},
                            unicastLocatorList = [_L | _],
                            changes_for_reader = CR} =
                  P
              | TL],
             Sent) ->
    RequestedKeys = [K || {K, #change_for_reader{status = S}} <- maps:to_list(CR), S == Filter],
    NewCR = send_selected_changes(RequestedKeys, Guid, HC, P),
    rec_send_changes(Filter, Guid, HC, TL, [P#reader_proxy{changes_for_reader = NewCR} | Sent]).

send_changes(Filter, Guid, HC, RP) ->
    rec_send_changes(Filter, Guid, HC, RP, []).

write_loop(#state{entity = #endPoint{guid = Guid},
                  history_cache = HC,
                  datawrite_period = P,
                  reader_proxies = RP,
                  push_mode = true} =
               S) ->
    erlang:send_after(P, self(), write_loop),
    ProxiesPushed = send_changes(unsent, Guid, HC, RP),
    S#state{reader_proxies = send_changes(requested, Guid, HC, ProxiesPushed)};
write_loop(#state{entity = #endPoint{guid = Guid},
                  history_cache = HC,
                  datawrite_period = P,
                  reader_proxies = RP} =
               S) ->
    erlang:send_after(P, self(), write_loop),
    S#state{reader_proxies = send_changes(requested, Guid, HC, RP)}.

h_new_change(D,
             #state{last_sequence_number = Last_SN,
                    entity = E,
                    history_cache = _C} =
                 S) ->
    SN = Last_SN + 1,
    Change =
        #cacheChange{kind = alive,
                     writerGuid = E#endPoint.guid,
                     instanceHandle = 0,
                     sequenceNumber = SN,
                     data = D},
    {Change, S#state{last_sequence_number = SN}}.

h_update_matched_readers(Proxies, #state{reader_proxies = RP, history_cache = C} = S) ->
    Valid_GUIDS = [G || #reader_proxy{guid = G} <- Proxies],
    ProxyStillValid =
        [Proxy || #reader_proxy{guid = G} = Proxy <- RP, lists:member(G, Valid_GUIDS)],
    NewProxies =
        [Proxy
         || #reader_proxy{guid = GUID} = Proxy <- Proxies,
            not lists:member(GUID, [G || #reader_proxy{guid = G} <- RP])],
    % add cache changes to the unsent list for the new added locators
    Changes = rtps_history_cache:get_all_changes(C),
    S#state{reader_proxies = ProxyStillValid ++ reset_reader_proxies(Changes, NewProxies)}.

h_matched_reader_add(Proxy,
                     #state{entity = #endPoint{guid = GUID},
                            reader_proxies = RP,
                            history_cache = C,
                            heatbeat_count = Count} =
                         S) ->
    Changes = rtps_history_cache:get_all_changes(C),
    NewProxies = reset_reader_proxies(Changes, [Proxy]),
    HB = build_heartbeat(GUID, C, Count),
    [send_heartbeat_to_reader(GUID#guId.prefix, HB, P) || P <- NewProxies],
    S#state{reader_proxies = RP ++ NewProxies, heatbeat_count = Count + 1}.

h_matched_reader_remove(Guid, #state{reader_proxies = RP} = S) ->
    S#state{reader_proxies = [P || #reader_proxy{guid = G} = P <- RP, G /= Guid]}.

reset_reader_proxies(Changes, RP) ->
    rec_reset_reader_proxies(Changes, RP, []).

rec_reset_reader_proxies(_, [], NewRP) ->
    NewRP;
rec_reset_reader_proxies(Changes, [RP | TL], NewProxies) ->
    ChangesForReaders = lists:foldl(
        fun(#cacheChange{sequenceNumber = SN}, Acc) ->
            maps:put(SN, #change_for_reader{status = unacknowledged}, Acc)
        end,
        #{},
        Changes),
    N_RP = RP#reader_proxy{changes_for_reader = ChangesForReaders},
    rec_reset_reader_proxies(Changes, TL, [N_RP | NewProxies]).

is_acked_by_reader(SN, #reader_proxy{changes_for_reader = Changes}) ->
    %[ io:format("~p with key = ~p\n",[S,Key]) || #change_for_reader{change_key=Key, status = S}=C <- Changes],
    case maps:get(SN, Changes, #change_for_reader{status = unacknowledged}) of
        #change_for_reader{status = acknowledged} -> true;
        _ -> false
    end.

h_is_acked_by_all(ChangeKey, #state{reader_proxies = RP}) ->
    lists:all(fun(Proxy) -> is_acked_by_reader(ChangeKey,Proxy) end, RP).

add_change_to_proxies(SN, Proxies, Push) ->
    rec_add_change_to_proxies(SN, Proxies, [], Push).

rec_add_change_to_proxies(_, [], NewPR, _) ->
    NewPR;
rec_add_change_to_proxies(SN, [Proxy | TL], NewProxies, Push) ->
    Status = case Push of
        true -> unsent;
        false -> unacknowledged
    end,
    ReaderChange = #change_for_reader{status = Status},
    ChangeMap = (Proxy#reader_proxy.changes_for_reader)#{SN => ReaderChange},
    New_PR = Proxy#reader_proxy{changes_for_reader = ChangeMap},
    rec_add_change_to_proxies(SN, TL, [New_PR | NewProxies], Push).

h_on_change_available({_, SN},
                      #state{entity = #endPoint{guid = Guid},
                             history_cache = HC,
                             reader_proxies = RP,
                             push_mode = Push} =
                          S) ->
    ReaderProxies = add_change_to_proxies(SN, RP, Push),
    % instantly send changes that are to be pushed
    ProxiesPushed = send_changes(unsent, Guid, HC, ReaderProxies),
    S#state{reader_proxies = ProxiesPushed}.

rm_change_from_proxies(Key, Proxies) ->
    rec_rm_change_from_proxies(Key, Proxies, []).

rec_rm_change_from_proxies(_, [], NewPR) ->
    NewPR;
rec_rm_change_from_proxies(SN, [Proxy | TL], NewProxies) ->
    NewChangeList = maps:remove(SN, Proxy#reader_proxy.changes_for_reader),
    New_PR = Proxy#reader_proxy{changes_for_reader = NewChangeList},
    rec_rm_change_from_proxies(SN, TL, [New_PR | NewProxies]).

h_on_change_removed(Key, #state{history_cache = _C, reader_proxies = RP} = S) ->
    S#state{reader_proxies = rm_change_from_proxies(Key, RP)}.

update_for_acknack([], _, _, _, S) ->
    S;
update_for_acknack([#reader_proxy{ready = IsReady, changes_for_reader = Changes} = Proxy | _],
                   Others, Missed, FinalFlag, S) ->
    NewChangeMap =
        maps:map(fun(SN, C) ->
                    case lists:member(SN, Missed) of
                        true ->
                            C#change_for_reader{status = requested};
                        false ->
                            case SN < lists:min(Missed) of
                                true ->
                                    C#change_for_reader{status = acknowledged};
                                false ->
                                    case SN > lists:max(Missed) of
                                        true ->
                                            C#change_for_reader{status = unacknowledged};
                                        false ->
                                            C
                                    end
                            end
                    end
                end,
                Changes),
    % a remote reader is considered ready to receive only after it sent an acknack with a final flag
    Readiness = not IsReady and (FinalFlag == 1),
    S#state{reader_proxies =
                [Proxy#reader_proxy{ready = Readiness, changes_for_reader = NewChangeMap} | Others]}.

h_receive_acknack(_, #state{reader_proxies = []} = S) ->
    S;
h_receive_acknack(#acknack{readerGUID = RID, final_flag = FF, sn_range = Single},
                  #state{reader_proxies = RP, history_cache = _Cache} = S)
    when is_integer(Single) ->
    Others = [P || #reader_proxy{guid = G} = P <- RP, G /= RID],
    update_for_acknack([P || #reader_proxy{guid = G} = P <- RP, G == RID], Others, [Single], FF, S);
h_receive_acknack(#acknack{readerGUID = RID, final_flag = FF, sn_range = Range},
                  #state{reader_proxies = RP, history_cache = _Cache} = S) ->
    Others = [P || #reader_proxy{guid = G} = P <- RP, G /= RID],
    update_for_acknack([P || #reader_proxy{guid = G} = P <- RP, G == RID], Others, Range, FF, S).

h_flush_all_changes(#state{entity = #endPoint{guid = #guId{prefix = Prefix}},
                           history_cache = HC,
                           datawrite_period = _P,
                           reader_proxies = RP}) ->
    send_changes(unsent, Prefix, HC, RP).
